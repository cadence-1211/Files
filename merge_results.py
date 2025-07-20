# File: launch_comparison.py
# Purpose: The main script to start the entire process with robust error handling.
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
        print(f"  ❌ ERROR: Input file not found: {input_file}")
        for file_handle in output_files:
            file_handle.close()
        return False

    for file_handle in output_files:
        file_handle.close()
    print(f"-> Finished sharding {input_file}.")
    return True

def monitor_jobs(job_ids):
    """Monitors LSF jobs until they are all done."""
    if not job_ids: return
    print("\n--- Part 4: Monitoring LSF Jobs ---")
    while True:
        try:
            result = subprocess.run(f"bjobs -o 'jobid stat' {' '.join(job_ids)}", shell=True, check=True, capture_output=True, text=True)
            output = result.stdout.strip().split('\n')
            
            # Count how many jobs are in DONE or EXIT status
            finished_count = sum(1 for line in output if 'DONE' in line or 'EXIT' in line)
            total_jobs = len(job_ids)
            
            print(f"\r-> Monitoring... {finished_count}/{total_jobs} jobs complete. (Checking again in 30s)", end="")
            
            if finished_count == total_jobs:
                print("\n✅ All jobs have finished.")
                break
        except subprocess.CalledProcessError:
            # This error can happen if bjobs finds no running jobs, meaning they are all done.
            print("\n✅ All jobs have finished.")
            break
        time.sleep(30)

def get_job_report(job_id):
    """Gets the runtime and memory usage for a completed job from bhist."""
    try:
        result = subprocess.run(f"bhist -l {job_id}", shell=True, check=True, capture_output=True, text=True)
        output = result.stdout
        runtime_match = re.search(r"Total CPU time used is\s+([\d.]+)\s+seconds", output)
        runtime = float(runtime_match.group(1)) if runtime_match else "N/A"
        mem_match = re.search(r"MAX MEM:\s+([\d.]+\s+[KMG]B);", output)
        memory = mem_match.group(1).strip() if mem_match else "N/A"
        return runtime, memory
    except (subprocess.CalledProcessError, AttributeError):
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
        shards = int(input("How many parallel jobs do you want to run? (e.g., 10, 20): "))
        python_command = input("Enter the full path to the Python command for LSF (press Enter for default): ")
        if not python_command:
            python_command = "/grid/common/pkgsData/python-v3.9.6t1/Linux/RHEL8.0-2019-x86_64/bin/python3.9"
            print(f"   Using default: {python_command}")
    except (ValueError, EOFError):
        print("\nInvalid input or cancelled. Exiting.")
        return

    # --- Part 2: Perform the sharding ---
    print("\n--- Part 2: Sharding Files ---")
    # Use absolute paths for everything to avoid issues on LSF nodes
    script_dir = os.path.abspath(os.getcwd())
    shards_dir = os.path.join(script_dir, "shards")

    instcol1 = list(map(int, instcol1_str.split(',')))
    instcol2 = list(map(int, instcol2_str.split(',')))
    
    if not shard_file(file1, instcol1, shards, shards_dir): return
    if not shard_file(file2, instcol2, shards, shards_dir): return
    print("✅ Sharding complete.")

    # --- Part 3: Automatically submit jobs to LSF ---
    print("\n--- Part 3: Submitting Jobs to LSF ---")
    
    # Verify the comparison script exists before we try to submit jobs
    compare_script_path = os.path.join(script_dir, "compare_adv.py")
    if not os.path.exists(compare_script_path):
        print(f"\n❌ FATAL ERROR: The comparison script 'compare_adv.py' was not found.")
        print(f"  Please ensure 'compare_adv.py' is in the same directory as this script.")
        print(f"  Expected location: {compare_script_path}")
        return

    results_dir = os.path.join(script_dir, "results")
    logs_dir = os.path.join(script_dir, "logs")
    os.makedirs(results_dir, exist_ok=True)
    os.makedirs(logs_dir, exist_ok=True)

    file1_basename = os.path.basename(file1)
    file2_basename = os.path.basename(file2)
    job_ids = []

    for i in range(shards):
        # Construct all paths as absolute to ensure LSF can find them
        log_path = os.path.join(logs_dir, f"output_{i}.log")
        file1_shard_path = os.path.join(shards_dir, f"{file1_basename}_shard_{i}.txt")
        file2_shard_path = os.path.join(shards_dir, f"{file2_basename}_shard_{i}.txt")
        output_prefix_path = os.path.join(results_dir, f"run_{i}")

        bsub_command = (
            f"bsub -n 2 -R 'rusage[mem=16G]' -o '{log_path}' "
            f"{python_command} {compare_script_path} "
            f"--file1 '{file1_shard_path}' "
            f"--file2 '{file2_shard_path}' "
            f"--instcol1 '{instcol1_str}' --valcol1 {valcol1} "
            f"--instcol2 '{instcol2_str}' --valcol2 {valcol2} "
            f"--output_prefix '{output_prefix_path}'"
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
            # This is the enhanced error reporting block
            print("\n" + "="*80)
            print(f"❌ ERROR: The 'bsub' command failed for shard {i}. LSF rejected the job.")
            print("This is often due to an issue with your LSF environment or the command parameters.")
            print("Please review the details below. You may need to consult your LSF administrator.")
            print("="*80)
            print(f"\n[INFO] Failed Command:\n{bsub_command}\n")
            print(f"[INFO] Exit Code: {e.returncode}")
            print(f"\n[INFO] LSF Output (stdout):\n{e.stdout}")
            print(f"\n[INFO] LSF Error (stderr):\n{e.stderr}")
            print("\n" + "="*80)
            print("Common reasons for failure:")
            print("  1. The Python path is incorrect for the LSF nodes.")
            print("  2. The resource request (-R 'rusage[mem=16G]') is invalid for your queues.")
            print("  3. You do not have permission to submit to the default LSF queue.")
            print("  4. A file path in the command is incorrect or not accessible from LSF nodes.")
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
    prefix = os.path.join(results_dir, "run")
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
