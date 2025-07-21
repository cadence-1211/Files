// High-Performance C++ File Comparison Tool
//
// This C++ application replicates the functionality of the high-performance Python script.
// It uses modern C++ multithreading to parse large files in parallel for maximum speed.
//
// How to Compile:
//   g++ -std=c++17 -O3 -pthread -o comparer main.cpp
//
// How to Run:
//   ./comparer --file1 <path> --instcol1 <cols> --valcol1 <col> --file2 <path> --instcol2 <cols> --valcol2 <col>
//   Example: ./comparer --file1 fileA.txt --instcol1 0,1 --valcol1 3 --file2 fileB.txt --instcol2 0,1 --valcol2 4
//
// If run without arguments, it will enter interactive mode.

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <chrono>
#include <variant>
#include <sstream>
#include <functional>
#include <mutex>
#include <future>

// A variant to hold either a numeric value (double) or a string value.
using ValueVariant = std::variant<double, std::string>;

// The main data structure to hold the parsed data for each instance.
// Key: A single string representing the combined instance key (e.g., "inst1|partA").
// Value: A pair containing the raw string value and the parsed ValueVariant.
using InstanceDataMap = std::unordered_map<std::string, std::pair<std::string, ValueVariant>>;

// A set to hold the unique instance keys for fast lookups.
using InstanceSet = std::unordered_set<std::string>;

// Set of keywords to identify metadata lines that should be skipped.
const std::unordered_set<std::string> METADATA_KEYWORDS = {
    "VERSION", "CREATION", "CREATOR", "PROGRAM", "DIVIDERCHAR", "DESIGN",
    "UNITS", "INSTANCE_COUNT", "NOMINAL_VOLTAGE", "POWER_NET", "GROUND_NET",
    "WINDOW", "RP_VALUE", "RP_FORMAT", "RP_INST_LIMIT", "RP_THRESHOLD",
    "RP_PIN_NAME", "MICRON_UNITS", "INST_NAME"
};

// Splits a string by a delimiter.
std::vector<std::string> split(const std::string& s, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(s);
    while (std::getline(tokenStream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

// Finds chunk boundaries in a file, ensuring chunks end on a newline.
std::vector<std::pair<long long, long long>> find_chunk_boundaries(const std::string& file_path, unsigned int num_chunks) {
    std::ifstream file(file_path, std::ios::binary | std::ios::ate);
    if (!file) {
        std::cerr << "❌ Error: Cannot open file '" << file_path << "'" << std::endl;
        return {};
    }

    long long file_size = file.tellg();
    if (file_size == 0) return {};

    std::vector<std::pair<long long, long long>> boundaries;
    long long chunk_size = file_size / num_chunks;
    long long current_pos = 0;

    for (unsigned int i = 0; i < num_chunks; ++i) {
        long long start = current_pos;
        long long end = (i == num_chunks - 1) ? file_size : start + chunk_size;

        file.seekg(end);
        std::string dummy;
        std::getline(file, dummy); // Read until newline
        end = file.tellg();
        if (end >= file_size || end == -1) {
            end = file_size;
        }

        if (start < end) {
            boundaries.push_back({start, end});
        }
        current_pos = end;
        if (current_pos >= file_size) break;
    }
    return boundaries;
}

// The core worker function executed by each thread.
std::pair<InstanceDataMap, InstanceSet> process_chunk(
    const std::string file_path,
    long long start_byte,
    long long end_byte,
    const std::vector<int> inst_cols,
    int value_col
) {
    InstanceDataMap data;
    InstanceSet instances_set;
    int max_col = 0;
    for (int col : inst_cols) max_col = std::max(max_col, col);
    max_col = std::max(max_col, value_col);

    std::ifstream file(file_path, std::ios::binary);
    file.seekg(start_byte);

    std::string line;
    while (file.tellg() < end_byte && std::getline(file, line)) {
        if (line.empty() || line[0] == '#' || line[0] == '\r') continue;

        std::stringstream ss(line);
        std::string first_word;
        ss >> first_word;
        if (METADATA_KEYWORDS.count(first_word)) continue;

        std::vector<std::string> parts;
        std::string part;
        // Reset stringstream to parse the full line
        ss.clear();
        ss.seekg(0);
        while (ss >> part) {
            parts.push_back(part);
        }

        if (parts.size() <= max_col) continue;

        try {
            std::string key_str;
            for (size_t i = 0; i < inst_cols.size(); ++i) {
                key_str += parts.at(inst_cols[i]);
                if (i < inst_cols.size() - 1) key_str += "|"; // Delimiter
            }

            std::string raw_val = parts.at(value_col);
            ValueVariant val_parsed;
            try {
                val_parsed = std::stod(raw_val);
            } catch (const std::invalid_argument&) {
                val_parsed = raw_val;
            }

            data[key_str] = {raw_val, val_parsed};
            instances_set.insert(key_str);
        } catch (const std::out_of_range&) {
            continue;
        }
    }
    return {data, instances_set};
}

// Orchestrates the parallel parsing of a file.
std::pair<InstanceDataMap, InstanceSet> parallel_parse_file(
    const std::string& file_path,
    const std::vector<int>& inst_cols,
    int value_col
) {
    unsigned int num_workers = std::thread::hardware_concurrency();
    std::cout << "\nParsing " << file_path << " with " << num_workers << " workers..." << std::endl;

    auto chunks = find_chunk_boundaries(file_path, num_workers);
    if (chunks.empty()) {
        std::cout << "Warning: File " << file_path << " is empty or could not be read." << std::endl;
        return {};
    }

    std::vector<std::future<std::pair<InstanceDataMap, InstanceSet>>> futures;
    for (const auto& chunk : chunks) {
        futures.push_back(std::async(std::launch::async, process_chunk, file_path, chunk.first, chunk.second, inst_cols, value_col));
    }

    InstanceDataMap final_data;
    InstanceSet final_instances_set;
    for (auto& fut : futures) {
        auto result = fut.get();
        final_data.insert(result.first.begin(), result.first.end());
        final_instances_set.insert(result.second.begin(), result.second.end());
    }

    return {final_data, final_instances_set};
}

// Writes the comparison CSV file.
void write_comparison_csv(
    const std::string& file1_name, const std::string& file2_name,
    const InstanceDataMap& data1, const InstanceDataMap& data2,
    const std::vector<std::string>& matched
) {
    std::cout << "Writing comparison.csv..." << std::endl;
    std::ofstream csvfile("comparison.csv");
    csvfile << "Key,Value_" << file1_name << ",Value_" << file2_name << ",Difference,Deviation_Match\n";

    for (const auto& key : matched) {
        const auto& pair1 = data1.at(key);
        const auto& pair2 = data2.at(key);

        csvfile << key << "," << pair1.first << "," << pair2.first << ",";

        if (std::holds_alternative<double>(pair1.second) && std::holds_alternative<double>(pair2.second)) {
            double val1 = std::get<double>(pair1.second);
            double val2 = std::get<double>(pair2.second);
            double diff = val1 - val2;
            csvfile << diff << ",";
            if (val2 != 0) {
                csvfile << (diff / val2) * 100 << "%";
            } else {
                csvfile << "inf";
            }
        } else {
            csvfile << "N/A," << (pair1.first == pair2.first ? "YES" : "NO");
        }
        csvfile << "\n";
    }
}

// Writes the missing instances file.
void write_missing_file(
    const std::string& file1_name, const std::string& file2_name,
    const std::vector<std::string>& miss2, const std::vector<std::string>& miss1
) {
    std::ofstream out("missing_instances.txt");
    out << "============================================================\n";
    out << "Instances missing from " << file2_name << ":\n";
    out << "============================================================\n";
    for (const auto& inst : miss2) out << inst << "\n";

    out << "\n============================================================\n";
    out << "Instances missing from " << file1_name << ":\n";
    out << "============================================================\n";
    for (const auto& inst : miss1) out << inst << "\n";
}


int main(int argc, char* argv[]) {
    std::unordered_map<std::string, std::string> args;
    // Simple argument parsing
    for (int i = 1; i < argc; i += 2) {
        if (i + 1 < argc) {
            args[argv[i]] = argv[i + 1];
        }
    }

    // Interactive mode if arguments are missing
    if (args.find("--file1") == args.end()) {
        std::cout << "Entering interactive mode...\n";
        std::cout << "Enter path to first file: ";
        std::cin >> args["--file1"];
        std::cout << "Enter instance match column indexes (e.g., 0,1) for file1: ";
        std::cin >> args["--instcol1"];
        std::cout << "Enter value column index for file1: ";
        std::cin >> args["--valcol1"];
        std::cout << "Enter path to second file: ";
        std::cin >> args["--file2"];
        std::cout << "Enter instance match column indexes (e.g., 0,1) for file2: ";
        std::cin >> args["--instcol2"];
        std::cout << "Enter value column index for file2: ";
        std::cin >> args["--valcol2"];
    }

    std::vector<int> instcol1, instcol2;
    int valcol1, valcol2;
    try {
        std::stringstream ss1(args["--instcol1"]);
        std::string segment;
        while(std::getline(ss1, segment, ',')) instcol1.push_back(std::stoi(segment));

        std::stringstream ss2(args["--instcol2"]);
        while(std::getline(ss2, segment, ',')) instcol2.push_back(std::stoi(segment));

        valcol1 = std::stoi(args["--valcol1"]);
        valcol2 = std::stoi(args["--valcol2"]);
    } catch (const std::exception& e) {
        std::cerr << "❌ Error: Invalid column arguments. Please provide comma-separated integers." << std::endl;
        return 1;
    }
    
    auto t_start = std::chrono::high_resolution_clock::now();

    auto result1 = parallel_parse_file(args["--file1"], instcol1, valcol1);
    auto result2 = parallel_parse_file(args["--file2"], instcol2, valcol2);

    InstanceDataMap data1 = result1.first;
    InstanceSet instances1 = result1.second;
    InstanceDataMap data2 = result2.first;
    InstanceSet instances2 = result2.second;

    std::cout << "\nComparing data..." << std::endl;
    std::vector<std::string> missing_in_file2, missing_in_file1, matched_instances;
    for (const auto& inst : instances1) {
        if (instances2.count(inst)) {
            matched_instances.push_back(inst);
        } else {
            missing_in_file2.push_back(inst);
        }
    }
    for (const auto& inst : instances2) {
        if (!instances1.count(inst)) {
            missing_in_file1.push_back(inst);
        }
    }
    std::sort(missing_in_file1.begin(), missing_in_file1.end());
    std::sort(missing_in_file2.begin(), missing_in_file2.end());
    std::sort(matched_instances.begin(), matched_instances.end());

    std::cout << "Writing output files..." << std::endl;
    std::string f1_basename = args["--file1"].substr(args["--file1"].find_last_of("/\\") + 1);
    std::string f2_basename = args["--file2"].substr(args["--file2"].find_last_of("/\\") + 1);

    write_missing_file(f1_basename, f2_basename, missing_in_file2, missing_in_file1);
    if (!matched_instances.empty()) {
        write_comparison_csv(f1_basename, f2_basename, data1, data2, matched_instances);
    } else {
        std::cout << "Note: No matched instances found; comparison.csv will be empty." << std::endl;
    }

    auto t_end = std::chrono::high_resolution_clock::now();
    double elapsed_time_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();

    std::cout << "\n===================================\n";
    std::cout << "✅ All tasks completed.\n";
    std::cout << "===================================\n";
    std::cout << "Instances in " << f1_basename << ": " << instances1.size() << "\n";
    std::cout << "Instances in " << f2_basename << ": " << instances2.size() << "\n";
    std::cout << "Matched Instances: " << matched_instances.size() << "\n";
    std::cout << "Missing from " << f2_basename << ": " << missing_in_file2.size() << "\n";
    std::cout << "Missing from " << f1_basename << ": " << missing_in_file1.size() << "\n";
    std::cout << "\nTotal execution time: " << elapsed_time_ms / 1000.0 << " seconds\n";

    return 0;
}
