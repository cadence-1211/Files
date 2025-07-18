# merge.py
# (This file is identical to the one in the previous answer. Copy it from there.)
import re
from pathlib import Path

def merge_results():
    """Merges all partial result files into final reports."""
    print(">>> Merging partial result files...")
    
    results_dir = Path("results")
    if not results_dir.exists():
        print("❌ Error: 'results' directory not found. Nothing to merge.", file=sys.stderr)
        return

    final_csv_path = Path("final_comparison.csv")
    csv_shards = sorted(list(results_dir.glob("run_*_comparison.csv")))
    
    if not csv_shards:
        print("Warning: No comparison CSV files found to merge.")
    else:
        with open(final_csv_path, "w") as final_csv:
            header = csv_shards[0].open().readline()
            final_csv.write(header)
            for shard_path in csv_shards:
                with open(shard_path, "r") as f:
                    f.readline()
                    final_csv.write(f.read())
        print(f"-> Merged {len(csv_shards)} CSV files into {final_csv_path}")

    final_missing_path = Path("final_missing_instances.txt")
    missing_shards = sorted(list(results_dir.glob("run_*_missing_instances.txt")))

    with open(final_missing_path, "w") as final_txt:
        final_txt.write(f"--- Combined Report of Missing Instances ---\n\n")
        for shard_path in missing_shards:
            final_txt.write(f"--- Results from {shard_path.name} ---\n")
            final_txt.write(shard_path.read_text())
            final_txt.write("\n")
    print(f"-> Merged {len(missing_shards)} missing instance files into {final_missing_path}")

def generate_summary():
    """Calculates and prints the final summary of the entire run."""
    print("\n>>> Generating Final Summary Report...")
    
    try:
        initial_counts_log = Path("stats/initial_counts.log").read_text()
        file1_lines = int(re.search(r"file1_lines=(\d+)", initial_counts_log).group(1))
        file2_lines = int(re.search(r"file2_lines=(\d+)", initial_counts_log).group(1))
    except (FileNotFoundError, AttributeError):
        file1_lines, file2_lines = "N/A", "N/A"

    total_missing1, total_missing2, total_comparison_lines = 0, 0, 0
    log_dir = Path("logs")
    if log_dir.exists():
        for log_file in log_dir.glob("output_*.log"):
            content = log_file.read_text()
            total_missing1 += sum(int(val) for val in re.findall(r"SUMMARY_STATS:missing_in_file1=(\d+)", content))
            total_missing2 += sum(int(val) for val in re.findall(r"SUMMARY_STATS:missing_in_file2=(\d+)", content))
            total_comparison_lines += sum(int(val) for val in re.findall(r"SUMMARY_STATS:comparison_lines=(\d+)", content))

    print("\n========= FINAL SUMMARY =========")
    print(f"Original Lines in file1.txt: {file1_lines:,}" if isinstance(file1_lines, int) else "N/A")
    print(f"Original Lines in file2.txt: {file2_lines:,}" if isinstance(file2_lines, int) else "N/A")
    print("-" * 20)
    print(f"Total Instances Missing from file1: {total_missing1:,}")
    print(f"Total Instances Missing from file2: {total_missing2:,}")
    print(f"Total Data Lines in final_comparison.csv: {total_comparison_lines:,}")
    print("=================================")

if __name__ == "__main__":
    merge_results()
    generate_summary()
