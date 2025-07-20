# File: merge_results.py
# Purpose: Merges all partial results into final output files.
import os
import argparse

def merge_csv_files(num_shards, prefix, final_filename):
    """Merges partial CSV files, handling the header correctly."""
    print(f"-> Merging {num_shards} comparison CSV files into '{final_filename}'...")
    
    first_file = f"{prefix}_0_comparison.csv"
    if not os.path.exists(first_file):
        print(f"  ERROR: Cannot find the first result file: {first_file}")
        print("  Did the LSF jobs run correctly? Check the 'logs' directory.")
        return False

    with open(final_filename, "w") as final_out:
        with open(first_file, "r") as f_in:
            final_out.write(f_in.read())
        
        for i in range(1, num_shards):
            partial_file = f"{prefix}_{i}_comparison.csv"
            if os.path.exists(partial_file):
                with open(partial_file, "r") as f_in:
                    next(f_in) # Skip the header line
                    final_out.write(f_in.read())
    return True

def merge_txt_files(num_shards, prefix, final_filename):
    """Concatenates all partial missing instance text files."""
    print(f"-> Merging {num_shards} missing instance TXT files into '{final_filename}'...")
    
    with open(final_filename, "w") as final_out:
        for i in range(num_shards):
            partial_file = f"{prefix}_{i}_missing_instances.txt"
            if os.path.exists(partial_file):
                with open(partial_file, "r") as f_in:
                    final_out.write(f_in.read())
                    final_out.write("\n")
    return True

def main():
    parser = argparse.ArgumentParser(description="Merge partial results from parallel LSF runs.")
    parser.add_argument("--shards", type=int, help="The number of shards/jobs that were run.")
    args = parser.parse_args()
    
    if not args.shards:
        try:
            args.shards = int(input("How many parallel jobs were run? (e.g., 5, 10, 20): "))
        except ValueError:
            print("Invalid input. Please enter a number.")
            return
            
    prefix = "results/run"
    final_csv = "final_comparison.csv"
    final_txt = "final_missing_instances.txt"

    print("\nStarting to merge results...")
    csv_ok = merge_csv_files(args.shards, prefix, final_csv)
    txt_ok = merge_txt_files(args.shards, prefix, final_txt)

    if csv_ok and txt_ok:
        print("\n✅ Merging complete! Your final files are ready:")
        print(f"  -> {final_csv}")
        print(f"  -> {final_txt}")

if __name__ == "__main__":
    m
