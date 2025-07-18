# compare_adv.py
# (This file is identical to the one in the previous answer. Copy it from there.)
import argparse
import os
import time
import sys
import mmap
import csv
import multiprocessing
import re

NUMERIC_RE = re.compile(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?")
METADATA_KEYWORDS_SET = {b"VERSION", b"CREATION", b"CREATOR", b"PROGRAM", b"DIVIDERCHAR"}

def is_valid_instance_line(line):
    line = line.strip()
    if not line or line.startswith(b"#"): return False
    for keyword in METADATA_KEYWORDS_SET:
        if line.startswith(keyword): return False
    return True

def extract_value(value_bytes, comparison_type):
    value_str = value_bytes.decode('utf-8', errors='ignore').strip()
    if comparison_type == 'numeric':
        match = NUMERIC_RE.search(value_str)
        if match:
            try: return float(match.group(0))
            except (ValueError, TypeError): return value_str
        else: return value_str
    else:
        return value_str

def parse_file_with_mmap(file_path, inst_cols, value_col, comparison_type):
    data, instances_set = {}, set()
    with open(file_path, "rb") as f:
        mmapped_file = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        for line in iter(mmapped_file.readline, b""):
            if not is_valid_instance_line(line): continue
            parts = line.strip().split()
            if len(parts) <= max(inst_cols + [value_col]): continue
            try:
                key = tuple(parts[i].decode('utf-8', errors='ignore').strip() for i in inst_cols)
                val_raw = parts[value_col].decode('utf-8', errors='ignore').strip()
                val_parsed = extract_value(parts[value_col], comparison_type)
                data[key] = (val_raw, val_parsed)
                instances_set.add(key)
            except IndexError: continue
        mmapped_file.close()
    return data, instances_set

def write_missing_file(file1_name, file2_name, miss2, miss1, out_filename):
    with open(out_filename, "w") as out:
        if miss2:
            out.writelines([f"Instances from '{file1_name}' missing in '{file2_name}':\n"])
            out.writelines(f"{' | '.join(inst)}\n" for inst in miss2)
        if miss1:
            out.writelines([f"\nInstances from '{file2_name}' missing in '{file1_name}':\n"])
            out.writelines(f"{' | '.join(inst)}\n" for inst in miss1)

def write_comparison_csv(file1_name, file2_name, data1, data2, matched, out_filename, comparison_type):
    if not matched: return 0
    lines_written = 0
    with open(out_filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        key_len = len(matched[0]) if matched else 1
        headers = [f"Instance_Key_{i+1}" for i in range(key_len)] + [f"{file1_name}_Value", f"{file2_name}_Value", "Difference", "Result"]
        writer.writerow(headers)
        for inst in matched:
            raw1, val1 = data1.get(inst, (None, None))
            raw2, val2 = data2.get(inst, (None, None))
            if comparison_type == 'numeric' and isinstance(val1, float) and isinstance(val2, float):
                diff = val1 - val2
                result = f"{abs((diff / val2) * 100):.2f}%" if val2 != 0 else ("Infinite %" if val1 != 0 else "0.00%")
                writer.writerow(list(inst) + [f"{val1:.4f}", f"{val2:.4f}", f"{diff:.4f}", result])
            else:
                match_result = "MATCH" if str(val1) == str(val2) else "MISMATCH"
                writer.writerow(list(inst) + [str(val1), str(val2), "N/A", match_result])
            lines_written += 1
    return lines_written

def parse_file_worker(args_tuple):
    return parse_file_with_mmap(*args_tuple)

def main():
    parser = argparse.ArgumentParser(description="Worker script for comparing two file shards on LSF.")
    parser.add_argument("--file1", required=True, help="Path to the first file shard.")
    parser.add_argument("--instcol1", required=True, help="Key columns for file 1.")
    parser.add_argument("--valcol1", required=True, type=int, help="Value column for file 1.")
    parser.add_argument("--file2", required=True, help="Path to the second file shard.")
    parser.add_argument("--instcol2", required=True, help="Key columns for file 2.")
    parser.add_argument("--valcol2", required=True, type=int, help="Value column for file 2.")
    parser.add_argument("--output_prefix", required=True, help="Prefix for output files.")
    parser.add_argument("--comparison_type", required=True, choices=['numeric', 'string'], help="Type of value comparison.")
    args = parser.parse_args()
    
    t0 = time.time()
    instcol1 = list(map(int, args.instcol1.strip().split(",")))
    instcol2 = list(map(int, args.instcol2.strip().split(",")))

    with multiprocessing.Pool(2) as pool:
        results = pool.map(parse_file_worker, [
            (args.file1, instcol1, args.valcol1, args.comparison_type),
            (args.file2, instcol2, args.valcol2, args.comparison_type)
        ])
    
    data1, instances1 = results[0]
    data2, instances2 = results[1]
    
    miss2 = sorted(list(instances1 - instances2))
    miss1 = sorted(list(instances2 - instances1))
    matched = sorted(list(instances1 & instances2))

    missing_filename = f"{args.output_prefix}_missing_instances.txt"
    comparison_filename = f"{args.output_prefix}_comparison.csv"
    
    write_missing_file(os.path.basename(args.file1), os.path.basename(args.file2), miss2, miss1, missing_filename)
    comparison_lines = write_comparison_csv(os.path.basename(args.file1), os.path.basename(args.file2), data1, data2, matched, comparison_filename, args.comparison_type)
    
    t1 = time.time()
    print(f"Run {args.output_prefix} finished in {t1 - t0:.2f} seconds.")
    
    print(f"SUMMARY_STATS:missing_in_file1={len(miss1)}")
    print(f"SUMMARY_STATS:missing_in_file2={len(miss2)}")
    print(f"SUMMARY_STATS:comparison_lines={comparison_lines}")

if __name__ == "__main__":
    main()
