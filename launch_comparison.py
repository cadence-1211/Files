# File: launch_comparison.py
# Purpose: The main script to start the entire process with automated monitoring.
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

    for file_handle in output_files:
        file_handle.close()
    print(f"-> Finished sharding {input_file}.")

def monitor_jobs(job_ids):
    """Monitors LSF jobs until they are all done."""
    print("\n--- Part 4: Monitoring Jobs ---")
    while True:
        finished_jobs = 0
        for job_id in job_ids:
            try:
                # The '-noheader' option simplifies parsing
                result = subprocess.run(f"bjobs -noheader {job_id}", shell=True, check=True, capture_output=True, text=True)
                if not result.stdout.strip():
                    finished_jobs += 1
                else:
                    # Job is still in the queue or running
                    pass
            except subprocess.CalledProcessError:
                # bjobs returns an error if the job is not found, which means it's finished
                finished_jobs += 1

        print(f"\r-> Monitoring... {finished_jobs}/{len(job_ids)} jobs complete.", end="")

        if finished_jobs == len(job_ids):
            print("\n✅ All jobs have finished.")
            break
        time.sleep(30) # Wait for 30 seconds before checking again

def get_job_report(job_id):
    """Gets the runtime and memory usage for a completed job."""
    try:
        result = subprocess.run(f"bjobs -l {job_id}", shell=True, check=True, capture_output=True, text=True)
        output = result.stdout

        runtime_match = re.search(r"Total CPU time used is\s+([\d.]+)\s+seconds", output)
        runtime = float(runtime_match.group(1)) if runtime_match else "N/A"

        mem_match = re.search(r"Max Memory\s+([\d.]+\s+[KMG]B)", output)
        memory = mem_match.group(1) if mem_match else "N/A"

        return runtime, memory
    except (subprocess.CalledProcessError, AttributeError):
        return "N/A", "N/A"


def main():
    """Guides the user, shards files, submits LSF jobs, monitors, and merges."""
    print("--- LSF Comparison Job Launcher (Automated) ---")

    # --- Part 1: Default Configuration ---
    print("\n--- Part 1: Using Default Configuration ---")
    file1 = "file1.txt"
    instcol1_str = "2"
    valcol1 = "1"

    file2 = "file2.txt"
    instcol2_str = "1"
    valcol2 = "1"

    shards = 10
    python_command = "/grid/common/pkgsData/python-v3.9.6t1/Linux/RHEL8.0-2019-x86_64/bin/python3.9"

    print(f"  File 1: {file1}, Key Column(s): {instcol1_str}, Value Column: {valcol1}")
    print(f"  File 2: {file2}, Key Column(s): {instcol2_str}, Value Column: {valcol2}")
    print(f"  Number of parallel jobs: {shards}")
    print(f"  Python command on LSF: {python_command}")


    # --- Part 2: Perform the sharding ---
    print("\n--- Part 2: Sharding Files ---")
    output_dir = "shards"
    instcol1 = list(map(int, instcol1_str.split(',')))
    instcol2 = list(map(int, instcol2_str.split(',')))

    shard_file(file1, instcol1, shards, output_dir)
    shard_file(file2, instcol2, shards, output_dir)
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
            f"--instcol1 '{instcol1_str}' "
            f"--valcol1 {valcol1} "
            f"--instcol2 '{instcol2_str}' "
            f"--valcol2 {valcol2} "
            f"--output_prefix 'results/run_{i}'"
        )

        try:
            print(f"-> Submitting job for shard {i}...")
            result = subprocess.run(bsub_command, shell=True, check=True, capture_output=True, text=True)
            match = re.search(r"Job <(\d+)>", result.stdout)
            if match:
                job_id = match.group(1)
                job_ids.append(job_id)
                print(f"   ...success! Job ID: {job_id}")
            else:
                print("   ERROR: Could not parse Job ID from bsub output.")
                print(f"   Output was: {result.stdout}")

        except subprocess.CalledProcessError as e:
            print(f"  ERROR: Failed to submit job for shard {i}. LSF command failed.")
            print(f"  Command was: {bsub_command}")
            print(f"  Stderr: {e.stderr}")
            break

    if not job_ids:
        print("\n No jobs were submitted. Exiting.")
        return

    # --- Part 4: Monitor Jobs ---
    monitor_jobs(job_ids)

    # --- Part 5: Generate Final Report ---
    print("\n--- Part 5: Final Job Report ---")
    total_runtime = 0
    for i, job_id in enumerate(job_ids):
        runtime, memory = get_job_report(job_id)
        if isinstance(runtime, float):
            total_runtime += runtime
        print(f"  - Shard {i} (Job {job_id}): Runtime={runtime}s, Max Memory={memory}")
    print(f"\n  Total CPU time for all jobs: {total_runtime:.2f} seconds")


    # --- Part 6: Merge results ---
    print("\n--- Part 6: Merging Results ---")
    prefix = "results/run"
    final_csv = "final_comparison.csv"
    final_txt = "final_missing_instances.txt"

    csv_ok = merge_results.merge_csv_files(shards, prefix, final_csv)
    txt_ok = merge_results.merge_txt_files(shards, prefix, final_txt)

    if csv_ok and txt_ok:
        print("\n✅ Process complete! Your final files are ready:")
        print(f"  -> {final_csv}")
        print(f"  -> {final_txt}")

if __name__ == "__main__":
    main()
