import psutil
import time
import csv
from datetime import datetime


exp = "."
log_file = "/home/seena/pcap/{}/rss_stats.csv".format(exp)
#log_file = '/home/seena/Projects/globus-stream/scistream-compute/src/analysis/rss_stats.csv'
#devname = "enp0s31f6" 
devname = "ens18"
timestep = 1   



def transmissionrate(rx_bytes, tx_bytes, dev, timestep):
    """Return the transmission rate of an interface under Linux in MB/s
    dev: nic name

    $ ls /sys/class/net/{}/statistics/
    collisions           rx_compressed        rx_errors            rx_length_errors     rx_over_errors       tx_bytes             tx_dropped           tx_heartbeat_errors
    multicast            rx_crc_errors        rx_fifo_errors       rx_missed_errors     rx_packets           tx_carrier_errors    tx_errors            tx_packets
    rx_bytes             rx_dropped           rx_frame_errors      rx_nohandler         tx_aborted_errors    tx_compressed        tx_fifo_errors       tx_window_errors
    """
    rx_path = "/sys/class/net/{}/statistics/rx_bytes".format(dev)
    tx_path = "/sys/class/net/{}/statistics/tx_bytes".format(dev)

    with open(rx_path, "r") as rx_f:
        rx_bytes[:] = rx_bytes[-1:] + [int(rx_f.read())]

    with open(tx_path, "r") as tx_f:
        tx_bytes[:] = tx_bytes[-1:] + [int(tx_f.read())]

    RX = (((rx_bytes[-1] - rx_bytes[-2]) / timestep) / (1024 * 1024) if len(rx_bytes) > 1 else 0)
    TX = (((tx_bytes[-1] - tx_bytes[-2]) / timestep) / (1024 * 1024) if len(tx_bytes) > 1 else 0)

    return RX, TX

def retransmission(rx_dropped, tx_dropped, dev):
    """Return the transmission rate of an interface under Linux in MB/s"""

    rx_path = "/sys/class/net/{}/statistics/rx_dropped".format(dev)
    tx_path = "/sys/class/net/{}/statistics/tx_dropped".format(dev)

    with open(rx_path, "r") as rx_f:
        rx_dropped[:] = rx_dropped[-1:] + [int(rx_f.read())]

    with open(tx_path, "r") as tx_f:
        tx_dropped[:] = tx_dropped[-1:] + [int(tx_f.read())]
        
    RX_DROP = (True if rx_dropped[-1] - rx_dropped[-2] else False) if len(rx_dropped) > 1 else False
    TX_DROP = (True if tx_dropped[-1] - tx_dropped[-2] else False) if len(tx_dropped) > 1 else False
 
    return RX_DROP, TX_DROP

def mem_cpu(prev_disk):
    """Get CPU, memory, and disk read/write since last check."""
    total_cpu = psutil.cpu_percent(percpu=True, interval=None)
    total_mem = psutil.virtual_memory().percent

    disk = psutil.disk_io_counters()
    total_disk_read = (disk.read_bytes - prev_disk.read_bytes) / (1024 * 1024)
    total_disk_write = (disk.write_bytes - prev_disk.write_bytes) / (1024 * 1024)

    return total_cpu, total_mem, total_disk_read, total_disk_write, disk

    
       

rx_bytes, tx_bytes = [], []
rx_dropped, tx_dropped = [], []

prev_disk = psutil.disk_io_counters()

with open(log_file, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "Timestamp", "Total CPU (%)", "Total Memory (%)",
        "Total Disk Read (MB)", "Total Disk Write (MB)",
        "Total Net RX (MB)", "Total Net TX (MB)", 
        "RX Dropped", "TX Dropped"
    ])

    print("Logging system-wide stats... Press Ctrl + C to stop.")
    next_run = time.time()

    try:
        while True:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            total_cpu, total_mem, total_disk_read, total_disk_write, prev_disk = mem_cpu(prev_disk)
            RX, TX = transmissionrate(rx_bytes, tx_bytes, devname, timestep)
            RX_DROP, TX_DROP = retransmission(rx_dropped, tx_dropped, devname)
            
            writer.writerow([
                timestamp, total_cpu, total_mem,
                total_disk_read, total_disk_write,
                RX, TX, RX_DROP, TX_DROP
            ])

            print(f"{timestamp} - CPU: {total_cpu}% | Mem: {total_mem:.1f}% | "
                  f"Disk R/W: {total_disk_read:.2f}/{total_disk_write:.2f} MB | "
                  f"NET RX/TX: {RX:.2f}/{TX:.2f} MB | "
                  f" RX Dropped: {RX_DROP} | TX Dropped: {TX_DROP}")

            # Wait until the next scheduled run time
            next_run += timestep
            sleep_time = next_run - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        print(f"\nLogging stopped. Data saved in {log_file}")