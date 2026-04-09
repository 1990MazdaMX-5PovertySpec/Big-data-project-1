import os
import csv
import time
import hashlib
import tempfile
import multiprocessing
from datetime import datetime
from math import sin, cos, sqrt, atan2, radians
import matplotlib.pyplot as plt
from memory_profiler import memory_usage

#Function that returns a valid datetime from a timestamp string
def parse_time(ts):
    return datetime.strptime(ts, "%d/%m/%Y %H:%M:%S")

#Function that converts kilometers to nautical miles
def km_to_nm(km):
    return km * 0.539957

#Function that safely converts a value to float, handling dirty data cases
def safe_float(val, default=0.0):
    if not val: return default
    try:
        clean_val = ''.join(c for c in str(val) if c.isdigit() or c == '.')
        return float(clean_val)
    except ValueError:
        return default

#Function that calculates the Haversine distance between two geographic coordinates
def coord_distance(lat_old, lon_old, lat_new, lon_new):
    R = 6373.0  # Radius of earth in km
    lat1, lon1 = radians(lat_old), radians(lon_old)
    lat2, lon2 = radians(lat_new), radians(lon_new)
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return round(R * c, 2)

#Function that processes a single partition file and computes vessel statistics 
#and anomalies A, C, D, and loitering sessions for anomaly B
def process_partition(partition_info):
    #Gets the temporary file path and worker ID from the partition information
    temp_file, worker_id = partition_info
    #Initializes dictionaries to store the last known state of each vessel and their statistics
    vessel_last_state = {}
    vessel_stats = {}

    #Checks if the temporary file exists, if not returns an empty dictionary
    if not os.path.exists(temp_file):
        return {}

    #Reads the partition file and processes it by row
    with open(temp_file, 'r') as f:
        #DictReader is used to read the CSV file as a dictionary
        reader = csv.DictReader(f)
        for row in reader:
            #Extracts relevant fields from the row
            mmsi = row['MMSI']
            current_time = parse_time(row['# Timestamp'])
            curr_lat = float(row['Latitude'])
            curr_lon = float(row['Longitude'])
            curr_draught = safe_float(row.get('Draught', 0))

            #Initializes vessel statistics if this is the first time we see this MMSI
            if mmsi not in vessel_stats:
                vessel_stats[mmsi] = {
                    "max_gap_s": 0, "total_jump_km": 0,
                    "draft_changes": 0, "anomalies": set(),
                    "loitering_sessions": [], "current_loiter": None
                }

            #If the vessel has been seen before, we look for anomalies
            if mmsi in vessel_last_state:
                prev = vessel_last_state[mmsi]

                #Ensuring that the new row is chronologically as well
                if current_time <= prev["last_time"]:
                    continue

                #Checking if there is a time difference between the current and previous record, if not the rows could be duplicates
                time_diff = round((current_time - prev["last_time"]).total_seconds(), 3)

                if time_diff <= 0:
                    continue

                #Calculating the distance between the current and previous coordinates and the speed in knots
                dist_km = coord_distance(prev["lat"], prev["lon"], curr_lat, curr_lon)
                speed_knots = (dist_km / time_diff * 1943.84) if time_diff > 0 else 0

                #Checking for anomaly A
                if time_diff > 14400 and dist_km > 0.1:
                    vessel_stats[mmsi]["anomalies"].add("A")
                    vessel_stats[mmsi]["max_gap_s"] = max(time_diff, vessel_stats[mmsi]["max_gap_s"])

                #Checking for anomaly D
                if speed_knots > 60:
                    vessel_stats[mmsi]["anomalies"].add("D")
                    vessel_stats[mmsi]["total_jump_km"] += dist_km

                #Checking for anomaly C
                if time_diff > 7200 and prev["draught"] > 0:
                    if abs(curr_draught - prev["draught"]) / prev["draught"] > 0.05:
                        vessel_stats[mmsi]["anomalies"].add("C")
                        vessel_stats[mmsi]["draft_changes"] += 1

                #Calculating loitering sessions for anomaly B
                if speed_knots < 1:
                    if vessel_stats[mmsi]["current_loiter"] is None:
                        vessel_stats[mmsi]["current_loiter"] = {
                            "start": prev["last_time"], "last": current_time,
                            "lat": curr_lat, "lon": curr_lon, "count": 1
                        }
                    else:
                        cl = vessel_stats[mmsi]["current_loiter"]
                        cl["last"] = current_time
                        cl["lat"] = (cl["lat"] * cl["count"] + curr_lat) / (cl["count"] + 1)
                        cl["lon"] = (cl["lon"] * cl["count"] + curr_lon) / (cl["count"] + 1)
                        cl["count"] += 1
                else:
                    cl = vessel_stats[mmsi]["current_loiter"]
                    if cl is not None:
                        if (cl["last"] - cl["start"]).total_seconds() > 7200:
                            vessel_stats[mmsi]["loitering_sessions"].append(cl)
                        vessel_stats[mmsi]["current_loiter"] = None

            #Updating the last known state of the vessel
            vessel_last_state[mmsi] = {
                "last_time": current_time, "lat": curr_lat,
                "lon": curr_lon, "draught": curr_draught
            }
    #After processing all rows, we check if there are any ongoing loitering sessions that need to be closed
    for mmsi, stats in vessel_stats.items():
        cl = stats["current_loiter"]
        if cl is not None and (cl["last"] - cl["start"]).total_seconds() > 7200:
            stats["loitering_sessions"].append(cl)
        del stats["current_loiter"]

    #Removing the temporary file after processing
    os.remove(temp_file)
    return vessel_stats


#Function that streams the input CSV files, partitions the data based on MMSI
#and writes the partitions to temporary files for parallel processing
def stream_and_partition(file_paths, num_workers, chunk_buffer_size):
    #Creating temporary files for each worker to store their respective partitions
    temp_dir = tempfile.gettempdir()
    partition_files = [os.path.join(temp_dir, f"part_{i}.csv") for i in range(num_workers)]
    file_handles = [open(pf, 'w', newline='') for pf in partition_files]
    writers = [csv.writer(fh) for fh in file_handles]

    #Initializing buffers for each worker to store rows before writing to disk
    buffers = {i: [] for i in range(num_workers)}
    header_written = False
    mmsi_idx = None

    #Streaming through each input file and partitioning the data based on the MMSI field
    for file_path in file_paths:
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                continue
            #Writing the header to each partition file only once
            if not header_written:
                for w in writers: w.writerow(header)
                mmsi_idx = header.index('MMSI')
                header_written = True

            #Processing each row in the input file and 
            #assigning it to the appropriate worker based on the hash of the MMSI
            for row in reader:
                if not row or len(row) <= mmsi_idx:
                    continue

                mmsi = row[mmsi_idx].strip()

                #Validating the MMSI to ensure it is made up of 9 digits, if not we skip the row
                if not (len(mmsi) == 9 and mmsi.isdigit()):
                    continue
                
                #Using a hash of the MMSI to determine which worker should process this row, 
                #ensuring that all rows for the same MMSI go to the same worker
                target_worker = int(hashlib.md5(mmsi.encode()).hexdigest(), 16) % num_workers
                buffers[target_worker].append(row)

                #If the buffer reaches the specified chunk size, the rows are written
                #and the buffer is cleared
                if len(buffers[target_worker]) >= chunk_buffer_size:
                    writers[target_worker].writerows(buffers[target_worker])
                    buffers[target_worker].clear()

    #After processing all input files, we write the remaining rows
    for i in range(num_workers):
        if buffers[i]: writers[i].writerows(buffers[i])
        file_handles[i].close()

    return [(partition_files[i], i) for i in range(num_workers)]


#Function that saves the computed vessel statistics 
#and anomalies to a CSV file in the output directory
def save_vessel_results(vessel_stats, output_file):
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['MMSI', 'Anomalies_Triggered', 'Max_Gap_Hrs', 'Total_Jump_NM', 'Draft_Changes', 'DFSI_Score'])

        #Iterates through each vessel's statistics and calculates the DFSI score
        for mmsi, stats in vessel_stats.items():
            if stats["anomalies"]:
                max_gap_hrs = stats["max_gap_s"] / 3600
                total_jump_nm = km_to_nm(stats["total_jump_km"])

                dfsi = (max_gap_hrs / 2) + (total_jump_nm / 10) + (stats["draft_changes"] * 15)
                if "B" in stats["anomalies"]: dfsi += 20

                anomalies_str = ", ".join(sorted(list(stats["anomalies"])))
                writer.writerow([
                    mmsi, anomalies_str, round(max_gap_hrs, 2),
                    round(total_jump_nm, 2), stats["draft_changes"], round(dfsi, 2)
                ])

#Function that runs the data partitioning and anomaly detection using multiporocessing
def run_parallel_system(file_paths, dir_output, num_workers, chunk_size):
    start_time = time.time()
    #Partitions the input data into temporary files for each worker to process in parallel
    parts = stream_and_partition(file_paths, num_workers, chunk_size)
    #Processes each partition file in parallel using a multiprocessing pool and collects the results
    with multiprocessing.Pool(num_workers) as pool:
        worker_results = pool.map(process_partition, parts)
    #Combines the results from all workers into a single dictionary
    combined_stats = {}
    for res in worker_results: combined_stats.update(res)

    #Checks for anomaly B by comparing loitering sessions across different vessels
    mmsis = list(combined_stats.keys())
    for i in range(len(mmsis)):
        mmsi1 = mmsis[i]
        sessions1 = combined_stats[mmsi1]["loitering_sessions"]
        if not sessions1: continue

        for j in range(i + 1, len(mmsis)):
            mmsi2 = mmsis[j]
            sessions2 = combined_stats[mmsi2]["loitering_sessions"]
            if not sessions2: continue

            for s1 in sessions1:
                for s2 in sessions2:
                    latest_start = max(s1["start"], s2["start"])
                    earliest_end = min(s1["last"], s2["last"])
                    if earliest_end > latest_start:
                        #Checking if the overlap is more than 2 hours
                        if (earliest_end - latest_start).total_seconds() > 7200:
                            dist_km = coord_distance(s1["lat"], s1["lon"], s2["lat"], s2["lon"])
                            #Check if the distance between vessels is less than 0.5 km
                            if dist_km <= 0.5:
                                combined_stats[mmsi1]["anomalies"].add("B")
                                combined_stats[mmsi2]["anomalies"].add("B")

    #Save the final results to a CSV file in the output directory
    save_vessel_results(combined_stats, os.path.join(dir_output, f'results_p{num_workers}_c{chunk_size}.csv'))
    return time.time() - start_time

#Function that generates performance charts
def plot_performance_metrics(mem_seq, mem_10k, mem_100k, t_seq, t_para, num_cores, output_dir):
    plt.switch_backend('Agg')
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    #Memory Usage Line Chart
    ax1.plot(mem_seq, color='#1f77b4', label='Sequential (1 Core)')
    ax1.plot(mem_10k, color='#2ca02c', label='Parallel (Chunk: 10k)')
    ax1.plot(mem_100k, color='#ff7f0e', label='Parallel (Chunk: 100k)')
    ax1.axhline(y=1024, color='red', linestyle='--', label='1GB Threshold')
    ax1.set_title('Memory Profile Comparison')
    ax1.set_ylabel('Memory (MB)')
    ax1.set_xlabel('Time (Samples)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    #Execution Time Bar Chart
    labels = ['Sequential (1 Core)', f'Parallel ({num_cores} Cores)']
    times = [t_seq, t_para]
    bars = ax2.bar(labels, times, color=['#1f77b4', '#2ca02c'])
    ax2.set_title('Execution Time Comparison')
    ax2.set_ylabel('Time (Seconds)')

    for bar in bars:
        yval = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width() / 2, yval + (yval * 0.02), f'{yval:.2f}s', ha='center', va='bottom',
                 fontweight='bold')

    plt.tight_layout()
    plot_path = os.path.join(output_dir, 'performance_metrics.png')
    plt.savefig(plot_path)
    print(f"-> Performance charts saved to {plot_path}")

#Main function
def main():
    #Change the input and output directories as needed
    dir_input, dir_output = "./input/", "./output/"
    os.makedirs(dir_output, exist_ok=True)

    #Collecting all CSV files from the input directory
    files = [os.path.join(dir_input, f) for f in os.listdir(dir_input) if f.endswith('.csv')]
    if not files: return print("Error: No CSV files found in ./input/")

    #Determining the number of CPU cores available for parallel processing
    num_cores = multiprocessing.cpu_count() - 1
    print(f"--- STARTING PERFORMANCE EVALUATION ({len(files)} files, {num_cores} cores) ---")
    #Running the code with a singe core as a baseline
    print("\n[1/3] Profiling Sequential Baseline...")
    mem_seq, t_seq = memory_usage(
        (run_parallel_system, (files, dir_output, 1, 100000)),
        multiprocess=True, include_children=True, retval=True
    )
    #Running the code in parallel with different chunk sizes to evaluate performance and memory usage
    print(f"[2/3] Profiling Parallel (Chunk Size: 5,000)...")
    mem_10k, t_10k = memory_usage(
        (run_parallel_system, (files, dir_output, num_cores, 5000)),
        multiprocess=True, include_children=True, retval=True
    )
    #Running the code in parallel with a larger chunk size to see if it improves performance or reduces memory usage
    print(f"[3/3] Profiling Parallel (Chunk Size: 50,000)...")
    mem_100k, t_100k = memory_usage(
        (run_parallel_system, (files, dir_output, num_cores, 50000)),
        multiprocess=True, include_children=True, retval=True
    )
    #Comparing the execution times of the sequential and parallel runs to calculate speedup and efficiency
    best_t_para = min(t_10k, t_100k)
    speedup = t_seq / best_t_para
    #Plotting the performance metrics for memory usage and execution time
    plot_performance_metrics(mem_seq, mem_10k, mem_100k, t_seq, best_t_para, num_cores, dir_output)
    #Printing final performance evaluation results to the console
    print("\n" + "=" * 35)
    print("FINAL HARDWARE EVALUATION")
    print("=" * 35)
    print(f"Sequential Time:  {t_seq:.2f}s")
    print(f"Parallel Time:    {best_t_para:.2f}s")
    print(f"Speedup Factor:   {speedup:.2f}x")
    print(f"Efficiency:       {(speedup / num_cores) * 100:.1f}%")


if __name__ == '__main__':
    main()