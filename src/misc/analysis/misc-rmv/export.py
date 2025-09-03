from scapy.all import rdpcap, TCP, UDP, IP
import pandas as pd
import numpy as np
from datetime import datetime

pcap_file = "swell/test_1/capture.pcap"
packets = rdpcap(pcap_file)

data = []
flows = {}  
syn_times = {} 
retransmissions = 0
prev_time = None 
start_time = None

for packet in packets:
    if IP in packet:
        entry = {
            "Timestamp": packet.time,
            "Src_IP": packet[IP].src,
            "Dst_IP": packet[IP].dst,
            "Packet_Size": len(packet),
            "Src_Port": "N/A",
            "Dst_Port": "N/A",
            "Protocol": "Other",
            "Throughput (Bytes/sec)": None,
            "Latency (sec)": None,
            "Jitter (sec)": None
        }

        # port numbers
        if TCP in packet:
            entry["Src_Port"] = packet[TCP].sport 
            entry["Dst_Port"] = packet[TCP].dport 
            entry["Protocol"] = "TCP"

        flow_id = (entry["Src_IP"], entry["Dst_IP"], entry["Protocol"], entry["Src_Port"], entry["Dst_Port"])

        if flow_id not in flows:
            flows[flow_id] = {
                "Packet_Count": 0,
                "Total_Bytes": 0,
                "Start_Time": packet.time,
                "End_Time": packet.time
            }

        flows[flow_id]["Packet_Count"] += 1
        flows[flow_id]["Total_Bytes"] += len(packet)
        flows[flow_id]["End_Time"] = packet.time

        if start_time is None:
            start_time = packet.time
        elapsed_time = packet.time - start_time
        throughput = sum(f["Total_Bytes"] for f in flows.values()) / elapsed_time if elapsed_time > 0 else 0
        entry["Throughput (Bytes/sec)"] = throughput

        if TCP in packet:
            if packet[TCP].flags & 0x10:  # ACK flag
                seq = packet[TCP].seq
                if seq in syn_times:
                    retransmissions += 1

            # Latency SYN → SYN-ACK → ACK
            if packet[TCP].flags & 0x02:  # SYN flag
                syn_times[(entry["Src_IP"], entry["Src_Port"])] = packet.time
            elif packet[TCP].flags & 0x12:  # SYN-ACK flag
                if (entry["Dst_IP"], entry["Dst_Port"]) in syn_times:
                    syn_time = syn_times[(entry["Dst_IP"], entry["Dst_Port"])]
                    entry["Latency (sec)"] = packet.time - syn_time
            elif packet[TCP].flags & 0x10 and len(data) > 0:  # ACK flag
                entry["Latency (sec)"] = data[-1]["Latency (sec)"]  # Carry forward last latency if no new handshake

        # jitter  
        if prev_time is not None:
            entry["Jitter (sec)"] = abs(packet.time - prev_time)
        prev_time = packet.time

        data.append(entry)

df = pd.DataFrame(data)

df = df.sort_values(by="Timestamp").reset_index(drop=True)

df["Timestamp"] = pd.to_numeric(df["Timestamp"], errors="coerce")
df["Timestamp"] = pd.to_datetime(df["Timestamp"], unit="s")

df = df.drop_duplicates(subset=["Timestamp"])

df = df.set_index("Timestamp").resample("10ms").asfreq().reset_index()

df.ffill(inplace=True)
df.bfill(inplace=True)

df.to_csv("network_analysis_aligned.csv", index=False)
print(f"network_analysis_aligned.csv\nTotal Packets: {len(packets)}\nRetransmissions: {retransmissions}")

def format_timestamp(ts):
    return datetime.utcfromtimestamp(float(ts)).strftime('%Y-%m-%d %H:%M:%S.%f')

flow_data = []
for flow_id, flow_stats in flows.items():
    start_time_fmt = format_timestamp(flow_stats["Start_Time"])
    end_time_fmt = format_timestamp(flow_stats["End_Time"])
    
    duration = pd.to_datetime(end_time_fmt) - pd.to_datetime(start_time_fmt)
    throughput = flow_stats["Total_Bytes"] / duration.total_seconds() if duration.total_seconds() > 0 else 0
    avg_packet_size = flow_stats["Total_Bytes"] / flow_stats["Packet_Count"]

    flow_data.append([
        *flow_id,  # Src_IP, Dst_IP, Protocol, Src_Port, Dst_Port
        flow_stats["Packet_Count"],
        flow_stats["Total_Bytes"],
        start_time_fmt,
        end_time_fmt,
        throughput,
        duration.total_seconds(),
        avg_packet_size
    ])

df_flows = pd.DataFrame(flow_data, columns=[
    "Src_IP", "Dst_IP", "Protocol", "Src_Port", "Dst_Port",
    "Packet_Count", "Total_Bytes", "Start_Time", "End_Time",
    "Throughput (Bytes/sec)", "Flow_Duration (sec)", "Avg_Packet_Size"
])

df_flows.to_csv("flow_stats_fixed.csv", index=False)
print("saved to flow_stats_fixed.csv")