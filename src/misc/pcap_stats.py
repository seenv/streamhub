from scapy.all import rdpcap, TCP, IP
import pandas as pd
import numpy as np
from datetime import datetime

pcap_file = "scistream.pcapng"
packets = rdpcap(pcap_file)

# Data containers
per_packet_data = []
per_flow_stats = {}
data_packets = {}  # key: (src_ip, src_port, dst_ip, dst_port, seq), value: timestamp
ack_latest = {}   # key: ack, value: latest ack timestamp
start_time = None

# RTT and jitter helpers
tsval_map = {}       # Maps (ip, port) -> (timestamp, TSval)
tsecr_lookup = {}    # Maps TSval -> original timestamp
prev_rtt_by_flow = {}  # key: flow_id, value: previous RTT (for jitter)

for packet in packets:
    if IP in packet and TCP in packet:
        timestamp = packet.time
        ip = packet[IP]
        tcp = packet[TCP]
        ttl = ip.ttl
        src_ip = ip.src
        dst_ip = ip.dst
        src_port = tcp.sport
        dst_port = tcp.dport
        size = len(packet)
        protocol = "TCP"
        rtt = None
        tsval, tsecr = None, None

        # extract TSval and TSecr from tcp options
        for opt in tcp.options:
            if opt[0] == 'Timestamp':
                tsval, tsecr = opt[1]

        if tsval is not None:
            tsval_map[(src_ip, src_port)] = (timestamp, tsval)
            tsecr_lookup[tsval] = timestamp

        if tsecr is not None and tsecr in tsecr_lookup:
            rtt = timestamp - tsecr_lookup[tsecr]

        key = (src_ip, src_port, dst_ip, dst_port, tcp.seq)
        if len(tcp.payload) > 0:
            data_packets[key] = timestamp
        elif tcp.flags & 0x10 and len(tcp.payload) == 0:
            ack_latest[tcp.ack] = max(ack_latest.get(tcp.ack, 0), timestamp)

        # flow id,, stats
        flow_id = (src_ip, dst_ip, protocol, src_port, dst_port)
        if flow_id not in per_flow_stats:
            per_flow_stats[flow_id] = {
                "packet_count": 0,
                "total_bytes": 0,
                "start_time": timestamp,
                "end_time": timestamp
            }

        flow = per_flow_stats[flow_id]
        flow["packet_count"] += 1
        flow["total_bytes"] += size
        flow["end_time"] = timestamp

        if start_time is None:
            start_time = timestamp
        elapsed = timestamp - start_time
        throughput = sum(f["total_bytes"] for f in per_flow_stats.values()) / elapsed if elapsed > 0 else 0

        # compute jitter based on RTT difference per flow
        prev_rtt = prev_rtt_by_flow.get(flow_id)
        jitter = abs(rtt - prev_rtt) if rtt is not None and prev_rtt is not None else None
        if rtt is not None:
            prev_rtt_by_flow[flow_id] = rtt

        # only record non-ACK-only packets
        if size != 66:
            per_packet_data.append({
                "timestamp": timestamp,
                "src_ip": src_ip,
                "dst_ip": dst_ip,
                "src_port": src_port,
                "dst_port": dst_port,
                "packet_size": size,
                "protocol": protocol,
                "ttl": ttl,
                "throughput": round(throughput / 1_000_000, 2),
                "latency": None,
                "jitter": round(jitter * 1_000_000, 2) if jitter else None,
                "rtt": round(rtt * 1_000_000, 2) if rtt else None
            })

# match each data packet to its latest ack
seq_to_index = {round(pkt["timestamp"], 6): i for i, pkt in enumerate(per_packet_data)}

for key, send_time in data_packets.items():
    _, _, _, _, seq = key
    if seq in ack_latest:
        ack_time = ack_latest[seq]
        latency = ack_time - send_time
        idx = seq_to_index.get(round(send_time, 6))
        if idx is not None:
            per_packet_data[idx]["latency"] = round(latency * 1_000_000, 2)

# Save per-packet CSV
df_packet = pd.DataFrame(per_packet_data)
df_packet["timestamp"] = pd.to_numeric(df_packet["timestamp"], errors="coerce")
df_packet["timestamp"] = pd.to_datetime(df_packet["timestamp"], unit="s")
df_packet.rename(columns={
    "timestamp": "Timestamp",
    "src_ip": "Src_IP",
    "dst_ip": "Dst_IP",
    "src_port": "Src_Port",
    "dst_port": "Dst_Port",
    "packet_size": "Packet_Size",
    "protocol": "Protocol",
    "ttl": "TTL",
    "throughput": "Throughput (MB/sec)",
    "latency": "Latency (µs)",
    "jitter": "Jitter (µs)",
    "rtt": "RTT (µs)"
}, inplace=True)
df_packet.to_csv("per_packet_stats.csv", index=False)
print("saved to per_packet_stats.csv")

# Save per-flow CSV
def format_timestamp(ts):
    return datetime.utcfromtimestamp(float(ts)).strftime('%Y-%m-%d %H:%M:%S.%f')

flow_records = []
for flow_id, stats in per_flow_stats.items():
    src_ip, dst_ip, proto, src_port, dst_port = flow_id
    duration = stats["end_time"] - stats["start_time"]
    duration_sec = duration if duration > 0 else 1e-9
    throughput = stats["total_bytes"] / duration_sec
    avg_pkt_size = stats["total_bytes"] / stats["packet_count"]

    flow_records.append({
        "Src_IP": src_ip,
        "Dst_IP": dst_ip,
        "Protocol": proto,
        "Src_Port": src_port,
        "Dst_Port": dst_port,
        "Packet_Count": stats["packet_count"],
        "Total_Bytes": stats["total_bytes"],
        "Start_Time": format_timestamp(stats["start_time"]),
        "End_Time": format_timestamp(stats["end_time"]),
        "Throughput (Bytes/sec)": throughput,
        "Flow_Duration (sec)": duration_sec,
        "Avg_Packet_Size": avg_pkt_size
    })

pd.DataFrame(flow_records).to_csv("per_flow_stats.csv", index=False)
print("saved to per_flow_stats.csv")

