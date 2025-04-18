#!/usr/bin/env python3

import os
import time
import psutil
import sys
import subprocess


def monitor_process(pid, interval=5):
    process = psutil.Process(pid)

    peak_memory = 0
    start_time = time.time()

    try:
        while process.is_running():
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)

            if memory_mb > peak_memory:
                peak_memory = memory_mb

            elapsed_time = time.time() - start_time

            sys.stderr.write(f"Memory usage: {memory_mb:.2f} MB, " +
                             f"Time elapsed: {elapsed_time:.2f} s\n")

            time.sleep(interval)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass

    sys.stderr.write(f"reporter:counter:PerformanceMetrics,PeakMemoryMB,{int(peak_memory)}\n")


def monitor_stdin_process(interval=5):

    pid = os.getpid()
    process = psutil.Process(pid)

    peak_memory = 0
    start_time = time.time()
    record_count = 0

    for line in sys.stdin:
        record_count += 1

        print(line.strip())


        if record_count % 1000 == 0:
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)

            if memory_mb > peak_memory:
                peak_memory = memory_mb

            elapsed_time = time.time() - start_time
            records_per_second = record_count / elapsed_time if elapsed_time > 0 else 0

            sys.stderr.write(f"Memory usage: {memory_mb:.2f} MB, Records processed: {record_count}, " +
                             f"Time elapsed: {elapsed_time:.2f} s, Records/sec: {records_per_second:.2f}\n")


    sys.stderr.write(f"reporter:counter:PerformanceMetrics,PeakMemoryMB,{int(peak_memory)}\n")
    sys.stderr.write(f"reporter:counter:PerformanceMetrics,TotalRecords,{record_count}\n")


def main():
    if len(sys.argv) > 1:
        command = sys.argv[1:]
        process = subprocess.Popen(command)
        monitor_process(process.pid)
        process.wait()
        return process.returncode
    else:
        monitor_stdin_process()
        return 0


if __name__ == "__main__":
    sys.exit(main())