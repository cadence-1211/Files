# File: launch_comparison.py
# Purpose: The main script to start the entire process.
import os
import argparse
import subprocess

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

def main():
    """Guides the user, shards files, and submits LSF jobs."""
    parser = argparse.ArgumentParser(description="Fully automated script to shard files and submit comparison jobs to LSF.")
    args = parser.parse_args()

    print("--- LSF Comparison Job Launcher ---")
    print("This script will guide you through sharding your files and submitting the jobs.")

    # --- Part 1: Gather all information interactively ---
    print("\n--- Part 1: Information Gathering ---")
    file1 = input("Enter path to first file: ")
    instcol1_str = input(f"Enter instance key column(s) for {os.path.basename(file1)} (e.g., 2): ")
    valcol1 = input(f"Enter value column for {os.path.basename(file1)} (e.g., 1): ")
    
    file2 = input("Enter path to second file: ")
    instcol2_str = input(f"Enter instance key column(s) for {os.path.basename(file2)} (e.g., 1): ")
    valcol2 = input(f"Enter value column for {os.path.basename(file2)} (e.g., 1): ")

    shards = int(input("How many parallel jobs do you want to run? (e.g., 5, 10, 20): "))
    python_command = input("Enter the full path to the Python command on LSF (press Enter for default): ")
    if not python_command:
        python_command = "/grid/common/pkgsData/python-v3.9.6t1/Linux/RHEL8.0-2019-x86_64/bin/python3.9"
        print(f"   Using default: {python_command}")

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
            subprocess.run(bsub_command, shell=True, check=True)
        except subprocess.CalledProcessError as e:
            print(f"  ERROR: Failed to submit job for shard {i}. LSF command failed.")
            print(f"  Command was: {bsub_command}")
            break

    print("\n✅ All jobs submitted! Check status with 'bjobs'.")
    print("Once all jobs are 'DONE', run 'python3 merge_results.py' to get the final output.")

if __name__ == "__main__":
    main()
