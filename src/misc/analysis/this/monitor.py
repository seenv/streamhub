import psutil
import time
import csv
from datetime import datetime

#log_file = "/home/seena/pcap/system_usage.csv"
log_file ='/home/seena/Projects/globus-stream/scistream-compute/test/log.csv'
with open(log_file, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Timestamp", "Total CPU (%)", "Total Memory (%)", 
                     "Total Disk Read (MB)", "Total Disk Write (MB)", 
                     "Total Net Sent (MB)", "Total Net Recv (MB)"])

    prev_disk = psutil.disk_io_counters()
    prev_net = psutil.net_io_counters()

    try:
        print("Logging system-wide stats... Press Ctrl + C to stop.")

        while True:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            total_cpu = psutil.cpu_percent(percpu=True, interval=1)
            total_mem = psutil.virtual_memory().percent

            disk = psutil.disk_io_counters()
            total_disk_read = (disk.read_bytes - prev_disk.read_bytes) / (1024 * 1024)
            total_disk_write = (disk.write_bytes - prev_disk.write_bytes) / (1024 * 1024)
            prev_disk = disk

            net = psutil.net_io_counters()
            total_net_sent = (net.bytes_sent - prev_net.bytes_sent) / (1024 * 1024)
            total_net_recv = (net.bytes_recv - prev_net.bytes_recv) / (1024 * 1024)
            prev_net = net

            writer.writerow([timestamp, total_cpu, total_mem, 
                             total_disk_read, total_disk_write, 
                             total_net_sent, total_net_recv])

            print(f"{timestamp} - CPU: {total_cpu}% | Mem: {total_mem}% | Disk R/W: {total_disk_read:.2f}/{total_disk_write:.2f} MB | Net Sent/Recv: {total_net_sent:.2f}/{total_net_recv:.2f} MB")

            time.sleep(5)

    except KeyboardInterrupt:
        print(f"\nLogging stopped. Data saved in {log_file}")
