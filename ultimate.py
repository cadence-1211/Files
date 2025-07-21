import argparse
import os
import time
import sys
import mmap
import csv
import multiprocessing
import re

METADATA_KEYWORDS = {
    b"VERSION", b"CREATION", b"CREATOR", b"PROGRAM", b"DIVIDERCHAR", b"DESIGN",
    b"UNITS", b"INSTANCE_COUNT", b"NOMINAL_VOLTAGE", b"POWER_NET", b"GROUND_NET",
    b"WINDOW", b"RP_VALUE", b"RP_FORMAT", b"RP_INST_LIMIT", b"RP_THRESHOLD",
    b"RP_PIN_NAME", b"MICRON_UNITS", b"INST_NAME"
}
METADATA_KEYWORDS_SET = set(METADATA_KEYWORDS)

def is_valid_instance_line(line):
    line = line.strip()
    if not line or line.startswith(b"#"):
        return False
    for keyword in METADATA_KEYWORDS_SET:
        if line.startswith(keyword):
            return False
    return True

def extract_value(value_bytes):
    value_str = value_bytes.decode('utf-8', errors='ignore').strip()
    # If it's a clean float, parse it
    if re.fullmatch(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", value_str):
        try:
            return float(value_str)
        except ValueError:
            pass
    return value_str  # Return as string if not float

def parse_file_with_mmap(file_path, inst_cols, value_col):
    data = {}
    instances_set = set()
    with open(file_path, "rb") as f:
        mmapped_file = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        chunk_size = 1024 * 1024
        buffer = b""
        while True:
            chunk = mmapped_file.read(chunk_size)
            if not chunk:
                break
            buffer += chunk
            lines = buffer.split(b"\n")
            buffer = lines.pop()
            for line in lines:
                if not is_valid_instance_line(line):
                    continue
                parts = line.strip().split()
                if len(parts) <= max(inst_cols + [value_col]):
                    continue
                try:
                    key = tuple(parts[i].decode('utf-8', errors='ignore').strip() for i in inst_cols)
                    val_raw = parts[value_col].decode('utf-8', errors='ignore').strip()
                    val_parsed = extract_value(parts[value_col])
                    data[key] = (val_raw, val_parsed)
                    instances_set.add(key)
                except:
                    continue
        if buffer:
            parts = buffer.strip().split()
            if len(parts) > max(inst_cols + [value_col]):
                try:
                    key = tuple(parts[i].decode('utf-8', errors='ignore').strip() for i in inst_cols)
                    val_raw = parts[value_col].decode('utf-8', errors='ignore').strip()
                    val_parsed = extract_value(parts[value_col])
                    data[key] = (val_raw, val_parsed)
                    instances_set.add(key)
                except:
                    pass
        mmapped_file.close()
    return data, instances_set

def compare_instances(data1, data2, instances1, instances2):
    missing_in_file2 = sorted([i for i in instances1 if i not in instances2])
    missing_in_file1 = sorted([i for i in instances2 if i not in instances1])
    matched = sorted(list(instances1 & instances2))
    return missing_in_file2, missing_in_file1, matched

def write_missing_file(file1_name, file2_name, miss2, miss1):
    with open("missing_instances.txt", "w") as out:
        out.writelines([
            f"{'='*60}\n",
            f"Instances missing from {file2_name}:\n",
            f"{'='*60}\n",
        ])
        out.writelines(f"{' | '.join(inst)}\n" for inst in miss2)
        out.writelines([
            f"\n{'='*60}\n",
            f"Instances missing from {file1_name}:\n",
            f"{'='*60}\n",
        ])
        out.writelines(f"{' | '.join(inst)}\n" for inst in miss1)

def write_comparison_csv(file1_name, file2_name, data1, data2, matched, col_name1, col_name2):
    with open("comparison.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        key_len = len(matched[0]) if matched else 1
        headers = [f"Key_{i+1}" for i in range(key_len)] + [
            f"{file1_name}_{col_name1}", f"{file2_name}_{col_name2}", "Difference", "Deviation / Match"
        ]
        writer.writerow(headers)
        for inst in matched:
            raw1, val1 = data1[inst]
            raw2, val2 = data2[inst]
            if isinstance(val1, float) and isinstance(val2, float):
                diff = val1 - val2
                deviation = (diff / val2) * 100 if val2 != 0 else float('inf')
                writer.writerow(list(inst) + [f"{val1:.4f}", f"{val2:.4f}", f"{diff:.4f}", f"{deviation:.2f}%"])
            else:
                match = "YES" if raw1 == raw2 else "NO"
                writer.writerow(list(inst) + [raw1, raw2, "N/A", match])

def get_column_name(file_path, col_index):
    with open(file_path, 'r') as f:
        for line in f:
            if line.strip() and not line.startswith("#"):
                headers = line.strip().split()
                if len(headers) > col_index:
                    return headers[col_index]
                else:
                    return f"Column {col_index + 1}"
    return f"Column {col_index + 1}"

def count_lines(path):
    with open(path, 'rb') as f:
        count = 0
        chunk_size = 1024 * 1024
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            count += chunk.count(b'\n')
    return count

def parse_file_worker(args):
    return parse_file_with_mmap(*args)

def main():
    parser = argparse.ArgumentParser(description="Compare two files and report missing instances + CSV comparison")
    parser.add_argument("--file1", help="Path to first file")
    parser.add_argument("--instcol1", help="Comma-separated instance column indexes in file1")
    parser.add_argument("--valcol1", type=int, help="0-based value column index in file1")
    parser.add_argument("--file2", help="Path to second file")
    parser.add_argument("--instcol2", help="Comma-separated instance column indexes in file2")
    parser.add_argument("--valcol2", type=int, help="0-based value column index in file2")
    args = parser.parse_args()

    if not args.file1:
        args.file1 = input("Enter path to first file: ")
    if not args.instcol1:
        args.instcol1 = input("Enter instance match column indexes (comma-separated) for file1: ")
    instcol1 = list(map(int, args.instcol1.strip().split(",")))
    if args.valcol1 is None:
        args.valcol1 = int(input("Enter value column index for file1: "))

    if not args.file2:
        args.file2 = input("Enter path to second file: ")
    if not args.instcol2:
        args.instcol2 = input("Enter instance match column indexes (comma-separated) for file2: ")
    instcol2 = list(map(int, args.instcol2.strip().split(",")))
    if args.valcol2 is None:
        args.valcol2 = int(input("Enter value column index for file2: "))

    if len(instcol1) != len(instcol2):
        print("❌ Error: Number of instance match columns must be the same in both files!")
        sys.exit(1)

    file1_name = os.path.basename(args.file1)
    file2_name = os.path.basename(args.file2)

    col_name1 = get_column_name(args.file1, args.valcol1)
    col_name2 = get_column_name(args.file2, args.valcol2)

    print("\nComparing Columns")
    print("=" * 35)
    print(f"  • From {file1_name}: {col_name1} (Column {args.valcol1 + 1})")
    print(f"  • From {file2_name}: {col_name2} (Column {args.valcol2 + 1})")

    t0 = time.time()
    lines1 = count_lines(args.file1)
    lines2 = count_lines(args.file2)

    with multiprocessing.Pool(2) as pool:
        results = pool.map(parse_file_worker, [
            (args.file1, instcol1, args.valcol1),
            (args.file2, instcol2, args.valcol2)
        ])

    data1, instances1 = results[0]
    data2, instances2 = results[1]

    miss2, miss1, matched = compare_instances(data1, data2, instances1, instances2)

    write_missing_file(file1_name, file2_name, miss2, miss1)
    write_comparison_csv(file1_name, file2_name, data1, data2, matched, col_name1, col_name2)

    t1 = time.time()
    print("\nSummary of Results")
    print("=" * 35)
    print(f"→ {len(miss1) + len(miss2)} missing instance(s) saved in 'missing_instances.txt'")
    print(f"→ {len(matched)} matched instance(s) saved in 'comparison.csv'")

    print("\nStatistics")
    print("=" * 35)
    print(f"  • Lines in {file1_name}: {lines1}")
    print(f"  • Lines in {file2_name}: {lines2}")
    print(f"  • Time elapsed         : {t1 - t0:.4f} seconds")

if __name__ == "__main__":
    m
