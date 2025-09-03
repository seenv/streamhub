import pyshark
import hashlib
import pandas as pd
from datetime import datetime

# === CONFIG ===
iface = 'enp0s31f6'  # change this to your interface if needed
ports = [5100, 5101, 5102, 5103, 5104, 5074, 5075, 5076, 5077, 5078]
ips = ['128.135.24.118', '128.135.24.120', '128.135.24.117', '128.135.24.119']
filter_expr = f"tcp and (port {' or port '.join(map(str, ports))})"

# === RETRANSMISSION TRACKER ===
connections = {}

def tcp_flags(flag_str):
    flags = int(flag_str, 16)
    if (flags & 0x02) and (flags & 0x10): return 'SYN-ACK'
    if (flags & 0x01) and (flags & 0x10): return 'FIN-ACK'
    if (flags & 0x08) and (flags & 0x10): return 'PSH-ACK'
    if flags & 0x01: return 'FIN'
    if flags & 0x02: return 'SYN'
    if flags & 0x04: return 'RST'
    if flags & 0x08: return 'PSH'
    if flags & 0x10: return 'ACK'
    if flags & 0x20: return 'URG'
    return 'UNKNOWN'

def hash_payload(payload):
    try:
        return hashlib.md5(bytes.fromhex(payload)).hexdigest() if payload else None
    except Exception:
        return None

def retransmission_check(ip_src, ip_dst, sport, dport, seq, payload_len, payload_hash):
    key = (ip_src, ip_dst, sport, dport)
    if key not in connections:
        connections[key] = []

    covered = connections[key]
    for item in covered:
        if item[0] == seq and item[1] == payload_len:
            if item[2] == payload_hash:
                print(f"Retransmission detected in {key} at Seq={seq}")
            else:
                print(f"Possible duplicate Seq={seq} with differing payload in {key}")
            return True

    covered.append((seq, payload_len, payload_hash))
    return False

# === MAIN PACKET PROCESSING ===
stream = []

print(f"Starting live capture on interface: {iface}")
capture = pyshark.LiveCapture(interface=iface, bpf_filter=filter_expr)

try:
    for packet in capture.sniff_continuously():
        try:
            ip_layer = packet.ip
            tcp_layer = packet.tcp

            if ip_layer.src not in ips and ip_layer.dst not in ips:
                continue

            timestamp = datetime.fromtimestamp(float(packet.sniff_timestamp))
            src_ip = ip_layer.src
            dst_ip = ip_layer.dst
            src_port = int(tcp_layer.srcport)
            dst_port = int(tcp_layer.dstport)
            seq = int(tcp_layer.seq)
            ack = int(tcp_layer.ack)
            flags = tcp_flags(tcp_layer.flags)
            payload_raw = getattr(tcp_layer, 'payload', '')
            payload_hash = hash_payload(payload_raw)
            payload_len = int(tcp_layer.len) if hasattr(tcp_layer, 'len') else 0
            is_retrans = retransmission_check(src_ip, dst_ip, src_port, dst_port, seq, payload_len, payload_hash)

            stream.append({
                'Timestamp': timestamp,
                'Src_IP': src_ip,
                'Dst_IP': dst_ip,
                'Src_Port': src_port,
                'Dst_Port': dst_port,
                'Packet_Seq': seq,
                'Packet_Ack': ack,
                'Packet_Size': payload_len,
                'Packet_Flags': flags,
                'Retransmission': is_retrans,
                'Payload_Hash': payload_hash
            })

            if len(stream) % 10 == 0:
                print(f"Captured {len(stream)} packets...")

        except Exception as per_packet_error:
            print(f"Error processing packet: {per_packet_error}")

except KeyboardInterrupt:
    print("Stopped by user.")

except Exception as e:
    print(f"Unexpected capture error: {e}")

finally:
    print("Capture complete. Cleaning up...")
    capture.close()

    if stream:
        df = pd.DataFrame(stream)
        df.to_csv("live_capture.csv", index=False)
        print(df.describe())
    else:
        print("No packets matched the filter or were captured.")
