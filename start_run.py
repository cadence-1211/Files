# start_run.py
import sys
import subprocess
import time
import re
from pathlib import Path

# --- Helper Functions for Sharding (moved from sharder.py) ---

def get_line_count(file_path):
    """Counts the number of lines in a file and reports it."""
    print(f"Counting lines in {file_path.name}...")
    try:
        with open(file_path, "r", errors='ignore') as f:
            count = sum(1 for _ in f)
        print(f"-> Found {count:,} lines.")
        return count
    except FileNotFoundError:
        print(f"❌ Error: File not found at {file_path}", file=sys.stderr)
        return None

def get_instance_key(line, key_cols):
    """Extracts the key from a line."""
    parts = line.strip().split()
    if len(parts) <= max(key_cols):
        return None
    return "_".join(parts[i] for i in key_cols)

def shard_file(input_file, key_cols, num_shards, output_dir):
    """Reads a large file and splits it into smaller shards based on a key."""
    print(f"Sharding {input_file.name}...")
    
    output_files = [open(output_dir / f"{input_file.name}_shard_{i}.txt", "w") for i in range(num_shards)]
    
    with open(input_file, "r", errors='ignore') as f:
        for line in f:
            if not line.strip() or line.strip().startswith("#"):
                continue
            
            key = get_instance_key(line, key_cols)
            if key is None:
                continue

            shard_index = hash(key) % num_shards
            output_files[shard_index].write(line)
            
    for file_handle in output_files:
        file_handle.close()
    print(f"-> Finished sharding {input_file.name}.")

# --- New function for interactive setup ---

def get_user_config():
    """Interactively prompts the user for all required configuration."""
    print("--- LSF Comparison Tool Setup ---")
    config = {}
    
    # File 1
    while 'file1' not in config:
        path = Path(input("1. Enter the path to the first file: ").strip())
        if path.is_file():
            config['file1'] = path
        else:
            print(f"   ❌ Error: File not found at '{path}'. Please try again.")
    config['instcol1'] = input("2. Enter instance key column(s) for file 1 (0-based, comma-separated, e.g., 0,1): ").strip()
    config['valcol1'] = input("3. Enter the value column for file 1 (0-based, e.g., 3): ").strip()
    
    print("-" * 20)
    
    # File 2
    while 'file2' not in config:
        path = Path(input("4. Enter the path to the second file: ").strip())
        if path.is_file():
            config['file2'] = path
        else:
            print(f"   ❌ Error: File not found at '{path}'. Please try again.")
    config['instcol2'] = input("5. Enter instance key column(s) for file 2 (0-based, comma-separated, e.g., 0,1): ").strip()
    config['valcol2'] = input("6. Enter the value column for file 2 (0-based, e.g., 3): ").strip()

    print("-" * 20)

    # Job and Comparison Type
    while 'shards' not in config:
        try:
            shards = int(input("7. How many parallel jobs to run on the cluster? (e.g., 5): ").strip())
            if shards > 0:
                config['shards'] = shards
            else:
                print("   ❌ Please enter a positive number.")
        except ValueError:
            print("   ❌ Invalid input. Please enter a number.")
            
    while 'comparison_type' not in config:
        choice = input("8. Choose comparison type ('numeric' or 'string'): ").strip().lower()
        if choice in ['numeric', 'string']:
            config['comparison_type'] = choice
        else:
            print("   ❌ Invalid input. Please enter 'numeric' or 'string'.")
            
    print("\n--- Configuration Complete. Starting process. ---\n")
    return config

# --- Helper Function for LSF Management (from launcher.py) ---

def submit_and_monitor(config):
    """Submits jobs to LSF and monitors them until completion."""
    print(">>> STEP 3: Submitting and Monitoring LSF Jobs...")
    
    # LSF settings
    PYTHON_EXEC = "python3"
    LSF_CORES = 2
    LSF_MEMORY = "8G"
    
    job_ids = {}

    for i in range(config['shards']):
        log_file = f"logs/output_{i}.log"
        output_prefix = f"results/run_{i}"

        cmd = [
            'bsub', '-n', str(LSF_CORES), '-R', f"rusage[mem={LSF_MEMORY}]", '-o', log_file,
            PYTHON_EXEC, "compare_adv.py",
            '--file1', f"shards/{config['file1'].name}_shard_{i}.txt",
            '--file2', f"shards/{config['file2'].name}_shard_{i}.txt",
            '--instcol1', config['instcol1'], '--valcol1', config['valcol1'],
            '--instcol2', config['instcol2'], '--valcol2', config['valcol2'],
            '--output_prefix', output_prefix,
            '--comparison_type', config['comparison_type']
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            match = re.search(r"<(\d+)>", result.stdout)
            if match:
                job_id = match.group(1)
                job_ids[job_id] = {'shard': i}
                print(f"  -> Submitted job {job_id} for shard {i}")
            else:
                print(f"  -> ❌ Failed to get job ID for shard {i}. LSF Output: {result.stdout}")
        except FileNotFoundError:
            print("❌ Error: 'bsub' command not found. Ensure LSF environment is loaded.", file=sys.stderr)
            sys.exit(1)
        except subprocess.CalledProcessError as e:
            print(f"❌ Error submitting job for shard {i}: {e.stderr}", file=sys.stderr)

    print("\n>>> All jobs submitted. Now monitoring completion...")
    print("----------------------------------------------------")
    
    completed_jobs = set()
    while len(completed_jobs) < len(job_ids):
        for job_id in job_ids:
            if job_id in completed_jobs: continue

            status_output = subprocess.run(['bjobs', '-o', 'stat', '-noheader', job_id], capture_output=True, text=True)
            status = status_output.stdout.strip()
            
            if status in ["DONE", "EXIT"]:
                print(f"✅ Job {job_id} (shard {job_ids[job_id]['shard']}) completed with status: {status}.")
                
                job_report = ""
                for _ in range(3):
                    time.sleep(3)
                    report_proc = subprocess.run(['bacct', '-l', job_id], capture_output=True, text=True)
                    if report_proc.returncode == 0 and "Accounting information about job" in report_proc.stdout:
                        job_report = report_proc.stdout
                        break
                
                cpu_time_match = re.search(r"CPU time\s*:\s*([\d.]+)\s*sec", job_report, re.IGNORECASE)
                mem_match = re.search(r"Max Memory\s*:\s*([\d.]+\s*[GMK]?B)", job_report, re.IGNORECASE)
                cpu_time = f"{cpu_time_match.group(1)} seconds" if cpu_time_match else "N/A"
                max_mem = mem_match.group(1).strip() if mem_match else "N/A"

                print(f"   - CPU Time: {cpu_time}")
                print(f"   - Max Memory: {max_mem}")
                print("----------------------------------------------------")
                completed_jobs.add(job_id)
            elif not status and job_id not in completed_jobs:
                completed_jobs.add(job_id)

        if len(completed_jobs) < len(job_ids):
            time.sleep(15)

    print("\nAll comparison jobs are complete. You can now run the merge script.")

# --- Main Execution Block ---

def main():
    try:
        # STEP 1: Get all settings from the user
        config = get_user_config()
        
        # Create directories for organization
        stats_dir = Path("stats")
        shards_dir = Path("shards")
        Path("logs").mkdir(exist_ok=True)
        Path("results").mkdir(exist_ok=True)
        stats_dir.mkdir(exist_ok=True)
        shards_dir.mkdir(exist_ok=True)
        
        # STEP 2: Count lines and shard the files
        print(">>> STEP 1: Analyzing and Sharding Source Files...")
        file1_lines = get_line_count(config['file1'])
        file2_lines = get_line_count(config['file2'])
        
        if file1_lines is None or file2_lines is None:
            sys.exit(1) # Exit if files were not found
        
        # Save stats for the final report
        with open(stats_dir / "initial_counts.log", "w") as f:
            f.write(f"file1_lines={file1_lines}\n")
            f.write(f"file2_lines={file2_lines}\n")
            
        print("-" * 20)
        
        instcol1 = list(map(int, config['instcol1'].split(',')))
        instcol2 = list(map(int, config['instcol2'].split(',')))
        
        shard_file(config['file1'], instcol1, config['shards'], shards_dir)
        shard_file(config['file2'], instcol2, config['shards'], shards_dir)

        print("\n>>> STEP 2: Sharding Complete.")
        
        # STEP 3: Submit and monitor LSF jobs
        submit_and_monitor(config)

    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user. Exiting.")
        sys.exit(0)
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
