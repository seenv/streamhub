import psutil
import csv
from datetime import datetime
import subprocess, os, argparse, json, time

"""log_file = './monitor_stats.csv'
devname = "eno1np0"
timestep = 1"""


def parse_args():
    parser = argparse.ArgumentParser(description="description",)
    #parser.add_argument("--log_path", type=str, required=True, help="Path to the log file for storing stats.")
    parser.add_argument("--log_path", type=str, default='./', help="Path to the log file for storing stats.")
    parser.add_argument("--devname", type=str, default="eno1np0", help="Network interface name to monitor.")
    parser.add_argument("--timestep", type=int, default=1, help="Time interval in seconds for logging stats.")

    return parser.parse_args()


def get_sysctl_value(param):
    try:
        return subprocess.check_output(["sysctl", "-n", param], encoding="utf-8").strip()
    except subprocess.CalledProcessError:
        return "N/A"


def get_mtu(interface):
    stats = psutil.net_if_stats()
    return stats[interface].mtu if interface in stats else "N/A"


def trans_rate(rx_bytes, tx_bytes, dev, timestep):
    rx_path = f"/sys/class/net/{dev}/statistics/rx_bytes"
    tx_path = f"/sys/class/net/{dev}/statistics/tx_bytes"

    with open(rx_path, "r") as rx_f:
        rx_bytes[:] = rx_bytes[-1:] + [int(rx_f.read())]

    with open(tx_path, "r") as tx_f:
        tx_bytes[:] = tx_bytes[-1:] + [int(tx_f.read())]

    RX = (((rx_bytes[-1] - rx_bytes[-2]) / timestep) / (1024 * 1024) if len(rx_bytes) > 1 else 0)
    TX = (((tx_bytes[-1] - tx_bytes[-2]) / timestep) / (1024 * 1024) if len(tx_bytes) > 1 else 0)

    return RX, TX


def retrans(rx_dropped, tx_dropped, dev):
    rx_path = f"/sys/class/net/{dev}/statistics/rx_dropped"
    tx_path = f"/sys/class/net/{dev}/statistics/tx_dropped"

    with open(rx_path, "r") as rx_f:
        rx_dropped[:] = rx_dropped[-1:] + [int(rx_f.read())]

    with open(tx_path, "r") as tx_f:
        tx_dropped[:] = tx_dropped[-1:] + [int(tx_f.read())]

    RX_DROP = (rx_dropped[-1] - rx_dropped[-2] != 0) if len(rx_dropped) > 1 else False
    TX_DROP = (tx_dropped[-1] - tx_dropped[-2] != 0) if len(tx_dropped) > 1 else False

    return RX_DROP, TX_DROP


def mem_cpu(prev_disk):
    per_cpu = psutil.cpu_percent(percpu=True, interval=None)
    total_mem = psutil.virtual_memory().percent

    disk = psutil.disk_io_counters()
    total_disk_read = (disk.read_bytes - prev_disk.read_bytes) / (1024 * 1024)
    total_disk_write = (disk.write_bytes - prev_disk.write_bytes) / (1024 * 1024)

    return per_cpu, total_mem, total_disk_read, total_disk_write, disk


def main():

    args = parse_args()
    rx_bytes, tx_bytes = [], []
    rx_dropped, tx_dropped = [], []
    prev_disk = psutil.disk_io_counters()
    #prev_disk = psutil.disk_io_counters(pernic=True)
    logs = os.path.join(args.log_path, 'sys_stats.jsonl')
    #logs = os.path.join(args.log_path, 'sys_stats.csv')
    
    metadata = {
        "tcp_congestion_control": get_sysctl_value("net.ipv4.tcp_congestion_control"),
        "rmem_max": get_sysctl_value("net.core.rmem_max"),
        "wmem_max": get_sysctl_value("net.core.wmem_max"),
        "tcp_rmem": get_sysctl_value("net.ipv4.tcp_rmem"),
        "tcp_wmem": get_sysctl_value("net.ipv4.tcp_wmem"),
        "interface_mtu": get_mtu(args.devname)
    }
    
    """
    writer.writerow(["System Info"])
    writer.writerow(["TCP Congestion Control", get_sysctl_value("net.ipv4.tcp_congestion_control")])
    writer.writerow(["net.core.rmem_max", get_sysctl_value("net.core.rmem_max")])
    writer.writerow(["net.core.wmem_max", get_sysctl_value("net.core.wmem_max")])
    writer.writerow(["net.ipv4.tcp_rmem", get_sysctl_value("net.ipv4.tcp_rmem")])
    writer.writerow(["net.ipv4.tcp_wmem", get_sysctl_value("net.ipv4.tcp_wmem")])
    writer.writerow(["Interface MTU", get_mtu(args.devname)])
    writer.writerow([])
    """
    
    # Header for performance stats
    num_cores = psutil.cpu_count()
    cpu_headers = [f"CPU Core {i} (%)" for i in range(num_cores)]
    """writer.writerow([
        "Timestamp", "Total CPU (%)", "Total Memory (%)",
        "Total Disk Read (MB)", "Total Disk Write (MB)",
        "Total Net RX (MB)", "Total Net TX (MB)",
        "RX Dropped", "TX Dropped"
    ] + cpu_headers)
    f.flush()"""

    print("Logging system-wide stats...")
    next_run = time.time()

    try:
        with open(logs, "a") as f:
        #with open(logs, "w", newline="") as f:
            f.write(json.dumps({"metadata": metadata}) + "\n")
            #writer = csv.writer(f)
            while True:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                per_cpu, total_mem, total_disk_read, total_disk_write, prev_disk = mem_cpu(prev_disk)
                total_cpu = sum(per_cpu) / len(per_cpu)
                RX, TX = trans_rate(rx_bytes, tx_bytes, args.devname, args.timestep)
                RX_DROP, TX_DROP = retrans(rx_dropped, tx_dropped, args.devname)

                log_entry = {
                    "timestamp": timestamp,
                    "total_cpu": round(total_cpu, 2),
                    "total_memory": round(total_mem, 2),
                    "disk_read_MB": round(total_disk_read, 2),
                    "disk_write_MB": round(total_disk_write, 2),
                    "net_rx_MB": round(RX, 2),
                    "net_tx_MB": round(TX, 2),
                    "rx_dropped": RX_DROP,
                    "tx_dropped": TX_DROP,
                    "per_cpu": [round(cpu, 2) for cpu in per_cpu]
                }
                f.write(json.dumps(log_entry) + "\n")
    
                """row = [
                    timestamp,
                    round(total_cpu, 2),
                    round(total_mem, 2),
                    round(total_disk_read, 2),
                    round(total_disk_write, 2),
                    round(RX, 2),
                    round(TX, 2),
                    RX_DROP,
                    TX_DROP
                ] + [round(cpu, 2) for cpu in per_cpu]

                writer.writerow(row)
                f.flush()"""

                print(f"{timestamp} - CPU: {total_cpu:.2f}% | Mem: {total_mem:.2f}% | "
                    f"Disk R/W: {total_disk_read:.2f}/{total_disk_write:.2f} MB | "
                    f"NET RX/TX: {RX:.2f}/{TX:.2f} MB | "
                    f"RX Dropped: {RX_DROP} | TX Dropped: {TX_DROP}")

                next_run += args.timestep
                sleep_time = next_run - time.time()
                if sleep_time > 0:
                    time.sleep(sleep_time)

    except KeyboardInterrupt:
        print(f"\nLogging stopped. Data saved in {logs}")
            
            
if __name__ == "__main__":
    main()