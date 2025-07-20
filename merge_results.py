# File: launch_comparison.py
# Purpose: The main script to start the entire process with user prompts and automated monitoring.
import os
import argparse
import subprocess
import time
import re
import merge_results # We will call the merge script directly

def get_instance_key(line, key_cols):
    """Extracts the key from a line for sharding."""
    parts = line.strip().split()
    if len(parts) <= max(key_cols):
        return None
    return "_".join(parts[i] for i in key_cols)

def shard_file(input_file, key_cols, num_shards, output_dir):
    """Reads a large file and splits it into smaller shards based on a key."""
    print(f"-> Processing {input_file}...")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"-> Created output directory: '{output_dir}'")

    output_files = [open(os.path.join(output_dir, f"{os.path.basename(input_file)}_shard_{i}.txt"), "w") for i in range(num_shards)]

    try:
        with open(input_file, "r", errors='ignore') as f:
            line_count = 0
            for line in f:
                line_count += 1
                if line_count % 5000000 == 0:
                    print(f"   ...processed {line_count // 1000000}M lines of {os.path.basename(input_file)}")
                if not line.strip() or line.strip().startswith("#"):
                    continue
                key = get_instance_key(line, key_cols)
                if key is None:
                    continue
                shard_index = hash(key) % num_shards
                output_files[shard_index].write(line)
    except FileNotFoundError:
        print(f"  ERROR: Input file not found: {input_file}")
        # Clean up created empty files
        for file_handle in output_files:
            file_handle.close()
        return False

    for file_handle in output_files:
        file_handle.close()
    print(f"-> Finished sharding {input_file}.")
    return True

def monitor_jobs(job_ids):
    """Monitors LSF jobs until they are all done."""
    if not job_ids:
        return
    print("\n--- Part 4: Monitoring LSF Jobs ---")
    while True:
        try:
            # Use bjobs to query the status of all jobs at once
            result = subprocess.run(f"bjobs {' '.join(job_ids)}", shell=True, check=True, capture_output=True, text=True)
            running_jobs = result.stdout.strip().split('\n')
            # The header line from bjobs is always present if there's at least one job
            num_active = len(running_jobs) - 1 if len(running_jobs) > 0 else 0
            num_finished = len(job_ids) - num_active
            
            print(f"\r-> Monitoring... {num_finished}/{len(job_ids)} jobs complete. (Checking again in 30s)", end="")

            if num_finished == len(job_ids):
                print("\n✅ All jobs have finished.")
                break

        except subprocess.CalledProcessError:
            # This error occurs when bjobs finds no jobs with the given IDs, meaning they are all done.
            print("\n✅ All jobs have finished.")
            break
        
        time.sleep(30) # Wait for 30 seconds before checking again


def get_job_report(job_id):
    """Gets the runtime and memory usage for a completed job from bhist."""
    try:
        # bhist is more reliable for jobs that have already finished
        result = subprocess.run(f"bhist -l {job_id}", shell=True, check=True, capture_output=True, text=True)
        output = result.stdout

        runtime_match = re.search(r"Total CPU time used is\s+([\d.]+)\s+seconds", output)
        runtime = float(runtime_match.group(1)) if runtime_match else "N/A"

        mem_match = re.search(r"MAX MEM:\s+([\d.]+\s+[KMG]B);", output)
        memory = mem_match.group(1).strip() if mem_match else "N/A"

        return runtime, memory
    except (subprocess.CalledProcessError, AttributeError):
        # Fallback to the log file if bhist fails (e.g., job info expired)
        return "N/A (check logs)", "N/A (check logs)"


def main():
    """Guides the user, shards files, submits LSF jobs, monitors, and merges."""
    parser = argparse.ArgumentParser(description="Fully automated script to shard files, submit comparison jobs to LSF, monitor progress, and merge results.")
    args = parser.parse_args()

    print("--- LSF Comparison Job Launcher (Interactive & Automated) ---")

    # --- Part 1: Gather all information interactively ---
    print("\n--- Part 1: Information Gathering ---")
    try:
        file1 = input("Enter path to the first file: ")
        instcol1_str = input(f"Enter instance key column(s) for {os.path.basename(file1)} (e.g., 2 or 0,1): ")
        valcol1 = input(f"Enter value column for {os.path.basename(file1)} (e.g., 1): ")
        
        file2 = input("Enter path to the second file: ")
        instcol2_str = input(f"Enter instance key column(s) for {os.path.basename(file2)} (e.g., 1 or 0,1): ")
        valcol2 = input(f"Enter value column for {os.path.basename(file2)} (e.g., 1): ")

        shards = int(input("How many parallel jobs do you want to run? (e.g., 5, 10, 20): "))
        python_command = input("Enter the full path to the Python command for LSF (press Enter for default): ")
        if not python_command:
            python_command = "/grid/common/pkgsData/python-v3.9.6t1/Linux/RHEL8.0-2019-x86_64/bin/python3.9"
            print(f"   Using default: {python_command}")
    except (ValueError, EOFError):
        print("\nInvalid input or cancelled. Exiting.")
        return

    # --- Part 2: Perform the sharding ---
    print("\n--- Part 2: Sharding Files ---")
    output_dir = "shards"
    instcol1 = list(map(int, instcol1_str.split(',')))
    instcol2 = list(map(int, instcol2_str.split(',')))
    
    if not shard_file(file1, instcol1, shards, output_dir): return
    if not shard_file(file2, instcol2, shards, output_dir): return
    print("✅ Sharding complete.")

    # --- Part 3: Automatically submit jobs to LSF ---
    print("\n--- Part 3: Submitting Jobs to LSF ---")
    os.makedirs("results", exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    file1_basename = os.path.basename(file1)
    file2_basename = os.path.basename(file2)
    job_ids = []

    for i in range(shards):
        bsub_command = (
            f"bsub -n 2 -R 'rusage[mem=16G]' -o 'logs/output_{i}.log' "
            f"{python_command} compare_adv.py "
            f"--file1 'shards/{file1_basename}_shard_{i}.txt' "
            f"--file2 'shards/{file2_basename}_shard_{i}.txt' "
            f"--instcol1 '{instcol1_str}' --valcol1 {valcol1} "
            f"--instcol2 '{instcol2_str}' --valcol2 {valcol2} "
            f"--output_prefix 'results/run_{i}'"
        )
        
        try:
            print(f"-> Submitting job for shard {i}...")
            result = subprocess.run(bsub_command, shell=True, check=True, capture_output=True, text=True)
            match = re.search(r"Job <(\d+)>", result.stdout)
            if match:
                job_id = match.group(1)
                job_ids.append(job_id)
                print(f"   ...Success! Job ID: {job_id}")
            else:
                print(f"  ERROR: Could not parse Job ID from bsub output. Full output:\n{result.stdout}")
        except subprocess.CalledProcessError as e:
            print(f"  ERROR: Failed to submit job for shard {i}. LSF command failed.")
            print(f"  Command was: {bsub_command}")
            print(f"  Stderr: {e.stderr}")
            break

    if len(job_ids) != shards:
        print("\n❌ Could not submit all jobs. Please check the LSF errors above. Exiting.")
        return

    # --- Part 4: Monitor Jobs ---
    monitor_jobs(job_ids)

    # --- Part 5: Generate Final Report ---
    print("\n--- Part 5: Final Job Report ---")
    total_runtime = 0.0
    for i, job_id in enumerate(job_ids):
        runtime, memory = get_job_report(job_id)
        if isinstance(runtime, float):
            total_runtime += runtime
        print(f"  - Shard {i} (Job {job_id}): Runtime={runtime}s, Max Memory={memory}")
    print(f"\n  Total CPU time for all jobs: {total_runtime:.2f} seconds")

    # --- Part 6: Merge Results Automatically ---
    print("\n--- Part 6: Merging Results ---")
    prefix = "results/run"
    final_csv = "final_comparison.csv"
    final_txt = "final_missing_instances.txt"

    csv_ok = merge_results.merge_csv_files(shards, prefix, final_csv)
    txt_ok = merge_results.merge_txt_files(shards, prefix, final_txt)

    if csv_ok and txt_ok:
        print("\n✅ Workflow complete! Your final files are ready:")
        print(f"  -> {final_csv}")
        print(f"  -> {final_txt}")

if __name__ == "__main__":
    main()
