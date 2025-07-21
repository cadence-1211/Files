import argparse
import os
import time
import sys
import mmap
import csv
import multiprocessing
import re

# Using a set of bytes is faster for checking prefixes.
# These keywords identify lines that are metadata, not instance data.
METADATA_KEYWORDS = {
    b"VERSION", b"CREATION", b"CREATOR", b"PROGRAM", b"DIVIDERCHAR", b"DESIGN",
    b"UNITS", b"INSTANCE_COUNT", b"NOMINAL_VOLTAGE", b"POWER_NET", b"GROUND_NET",
    b"WINDOW", b"RP_VALUE", b"RP_FORMAT", b"RP_INST_LIMIT", b"RP_THRESHOLD",
    b"RP_PIN_NAME", b"MICRON_UNITS", b"INST_NAME"
}

def find_chunk_boundaries(file_path, num_chunks):
    """
    Divides a file into byte-offset chunks that align with newlines.
    This ensures that no process starts reading in the middle of a line.
    """
    try:
        file_size = os.path.getsize(file_path)
    except FileNotFoundError:
        print(f"❌ Error: File not found at '{file_path}'")
        sys.exit(1)
        
    if file_size == 0:
        return []

    chunk_size = file_size // num_chunks
    boundaries = [0]
    with open(file_path, "rb") as f:
        for i in range(1, num_chunks):
            # Seek to the approximate chunk boundary
            seek_pos = min(chunk_size * i, file_size - 1)
            f.seek(seek_pos)
            f.readline()  # Read until the next newline to get a clean break
            boundaries.append(f.tell())
    boundaries.append(file_size)
    
    # Return (start_byte, end_byte) pairs for each chunk
    return [(boundaries[i], boundaries[i+1]) for i in range(len(boundaries)-1) if boundaries[i] < boundaries[i+1]]

def process_chunk(args):
    """
    Worker function: This is the core task executed by each process in the pool.
    It parses a specific byte chunk of a file, extracting instance data.
    """
    file_path, start_byte, end_byte, inst_cols, value_col = args
    max_col = max(inst_cols + [value_col])
    
    data = {}
    instances_set = set()

    with open(file_path, "rb") as f:
        # Use memory mapping for efficient read access within the specific chunk
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            mm.seek(start_byte)
            
            while mm.tell() < end_byte:
                line = mm.readline()
                if not line:
                    break
                
                stripped_line = line.strip()
                # Fast filtering for comments, empty lines, and metadata keywords
                if not stripped_line or stripped_line.startswith(b'#') or stripped_line.split(b' ', 1)[0] in METADATA_KEYWORDS:
                    continue

                parts = stripped_line.split()
                if len(parts) <= max_col:
                    continue
                
                try:
                    # Build the key tuple directly from bytes for max speed.
                    key = tuple(parts[i] for i in inst_cols)
                    value_bytes = parts[value_col]
                    
                    # Attempt to parse value as float, otherwise keep as string
                    try:
                        val_parsed = float(value_bytes)
                    except ValueError:
                        val_parsed = value_bytes.decode('utf-8', 'ignore')

                    data[key] = (value_bytes, val_parsed)
                    instances_set.add(key)
                except IndexError:
                    # This handles rare cases of malformed lines
                    continue

    return data, instances_set

def parallel_parse_file(file_path, inst_cols, value_col):
    """
    Orchestrates the parallel parsing of a single file.
    It divides the file into chunks and distributes them to a pool of worker processes.
    """
    num_workers = multiprocessing.cpu_count()
    print(f"Parsing {os.path.basename(file_path)} with {num_workers} workers...")
    
    # 1. Divide the file into chunks that respect line boundaries
    chunk_boundaries = find_chunk_boundaries(file_path, num_workers)
    if not chunk_boundaries:
        print(f"Warning: File {os.path.basename(file_path)} is empty or could not be read.")
        return {}, set()

    # 2. Prepare arguments for each worker process
    worker_args = [(file_path, start, end, inst_cols, value_col) for start, end in chunk_boundaries]
    
    # 3. Create a process pool and distribute the work
    with multiprocessing.Pool(processes=num_workers) as pool:
        results = pool.map(process_chunk, worker_args)

    # 4. Aggregate the results from all worker processes into a single dataset
    final_data = {}
    final_instances_set = set()
    for data_chunk, instances_chunk in results:
        final_data.update(data_chunk)
        final_instances_set.update(instances_chunk)
        
    return final_data, final_instances_set

def compare_instances(instances1, instances2):
    """Finds matched and missing instances between two sets."""
    missing_in_file2 = sorted(list(instances1 - instances2))
    missing_in_file1 = sorted(list(instances2 - instances1))
    matched = sorted(list(instances1 & instances2))
    return missing_in_file2, missing_in_file1, matched

def write_missing_file(file1_name, file2_name, miss2, miss1):
    """Writes the lists of missing instances to a text file."""
    with open("missing_instances.txt", "w", encoding='utf-8') as out:
        out.write(f"{'='*60}\nInstances missing from {file2_name}:\n{'='*60}\n")
        # Decode keys from bytes to string for readable output
        for inst in miss2:
            out.write(f"{' | '.join(k.decode('utf-8', 'ignore') for k in inst)}\n")
        
        out.write(f"\n{'='*60}\nInstances missing from {file1_name}:\n{'='*60}\n")
        for inst in miss1:
            out.write(f"{' | '.join(k.decode('utf-8', 'ignore') for k in inst)}\n")

def write_comparison_csv(file1_name, file2_name, data1, data2, matched, col_name1, col_name2):
    """Writes the detailed comparison of matched instances to a CSV file."""
    with open("comparison.csv", "w", newline="", encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        key_len = len(matched[0]) if matched else 1
        headers = [f"Key_{i+1}" for i in range(key_len)] + [
            f"{file1_name}_{col_name1}", f"{file2_name}_{col_name2}", "Difference", "Deviation / Match"
        ]
        writer.writerow(headers)
        
        for inst_key in matched:
            raw_bytes1, val1 = data1[inst_key]
            raw_bytes2, val2 = data2[inst_key]
            
            # Decode key tuple from bytes to string for CSV output
            key_list = [k.decode('utf-8', 'ignore') for k in inst_key]
            
            if isinstance(val1, float) and isinstance(val2, float):
                diff = val1 - val2
                deviation = (diff / val2) * 100 if val2 != 0 else float('inf')
                writer.writerow(key_list + [f"{val1:.4f}", f"{val2:.4f}", f"{diff:.4f}", f"{deviation:.2f}%"])
            else:
                # Decode raw values for string comparison and output
                raw1_str = raw_bytes1.decode('utf-8', 'ignore')
                raw2_str = raw_bytes2.decode('utf-8', 'ignore')
                match_status = "YES" if raw1_str == raw2_str else "NO"
                writer.writerow(key_list + [raw1_str, raw2_str, "N/A", match_status])

def get_column_name(file_path, col_index):
    """Quickly reads the first valid line of a file to get the column header name."""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                if line.strip() and not line.startswith("#"):
                    headers = line.strip().split()
                    return headers[col_index] if len(headers) > col_index else f"Column_{col_index + 1}"
    except (FileNotFoundError, IndexError):
        return f"Column_{col_index + 1}"

def main():
    """Main function to parse arguments, run processing, and print summaries."""
    parser = argparse.ArgumentParser(
        description="Compare two large text files efficiently using parallel processing.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--file1", help="Path to first file.")
    parser.add_argument("--instcol1", help="Comma-separated 0-based instance column indexes for file1.")
    parser.add_argument("--valcol1", type=int, help="0-based value column index for file1.")
    parser.add_argument("--file2", help="Path to second file.")
    parser.add_argument("--instcol2", help="Comma-separated instance column indexes for file2.")
    parser.add_argument("--valcol2", type=int, help="0-based value column index for file2.")
    args = parser.parse_args()

    # --- Interactive Input Section (restored from original script) ---
    if not args.file1:
        args.file1 = input("Enter path to first file: ")
    if not os.path.exists(args.file1):
        print(f"❌ Error: File not found at '{args.file1}'")
        sys.exit(1)

    if not args.instcol1:
        args.instcol1 = input("Enter instance match column indexes (e.g., 0,1) for file1: ")

    if args.valcol1 is None:
        try:
            args.valcol1 = int(input("Enter value column index for file1: "))
        except ValueError:
            print("❌ Error: Value column must be an integer.")
            sys.exit(1)

    if not args.file2:
        args.file2 = input("Enter path to second file: ")
    if not os.path.exists(args.file2):
        print(f"❌ Error: File not found at '{args.file2}'")
        sys.exit(1)

    if not args.instcol2:
        args.instcol2 = input("Enter instance match column indexes (e.g., 0,1) for file2: ")

    if args.valcol2 is None:
        try:
            args.valcol2 = int(input("Enter value column index for file2: "))
        except ValueError:
            print("❌ Error: Value column must be an integer.")
            sys.exit(1)
    # --- End of Interactive Input Section ---

    try:
        instcol1 = list(map(int, args.instcol1.strip().split(',')))
        instcol2 = list(map(int, args.instcol2.strip().split(',')))
    except (ValueError, AttributeError):
        print("❌ Error: Instance columns must be a comma-separated list of integers.")
        sys.exit(1)

    if len(instcol1) != len(instcol2):
        print("❌ Error: The number of instance match columns must be the same for both files.")
        sys.exit(1)

    t0 = time.time()
    file1_name = os.path.basename(args.file1)
    file2_name = os.path.basename(args.file2)

    # --- PARALLEL PROCESSING START ---
    data1, instances1 = parallel_parse_file(args.file1, instcol1, args.valcol1)
    data2, instances2 = parallel_parse_file(args.file2, instcol2, args.valcol2)
    # --- PARALLEL PROCESSING END ---

    print("\nComparing data...")
    miss2, miss1, matched = compare_instances(instances1, instances2)

    print("Writing output files...")
    col_name1 = get_column_name(args.file1, args.valcol1)
    col_name2 = get_column_name(args.file2, args.valcol2)
    write_missing_file(file1_name, file2_name, miss2, miss1)
    if matched:
        write_comparison_csv(file1_name, file2_name, data1, data2, matched, col_name1, col_name2)
    else:
        print("Note: No matched instances found; comparison.csv will be empty or not created.")

    t1 = time.time()
    
    print("\n" + "="*35)
    print("✅ All tasks completed.")
    print("="*35)
    print(f"Instances in {file1_name}: {len(instances1):,}")
    print(f"Instances in {file2_name}: {len(instances2):,}")
    print(f"Matched Instances: {len(matched):,}")
    print(f"Missing from {file2_name}: {len(miss2):,}")
    print(f"Missing from {file1_name}: {len(miss1):,}")
    print(f"\nTotal execution time: {t1 - t0:.4f} seconds")

if __name__ == "__main__":
    # This guard is essential for multiprocessing to work correctly on all platforms.
    # It prevents child processes from re-executing the main script's code.
    multiprocessing.freeze_support() # For compatibility with Windows executables
    main()
