from scapy.all import rdpcap, TCP, UDP, IP, Ether, RawPcapReader
import pandas as pd
import numpy as np
from datetime import datetime
import pyshark
import hashlib
import matplotlib.pyplot as plt
import matplotlib as mpl


#exp = "perf_P1x1_5100"
exp = "perf_P3x1_5104"
pcap = "/home/seena/pcap/{}/iperf.pcap".format(exp)

ports = [5100, 5101, 5102, 5103, 5104,
         5074, 5075, 5076, 5077, 5078]
ips = ['128.135.24.118', '128.135.24.120',
       '128.135.24.117', '128.135.24.119']
stream = []

#in retransmission:
connections = {}
seq_tracker = set()
covered = []

def tcp_flags(flags):
    """FIN-ACK  AF  0x011 | FIN	F	0x01 | SYN-ACK	AS	0x012 | SYN	S	0x02 | RST	R	0x04 | PSH	P	0x08 | ACK	A	0x10 | URG	U	0x20 """
    if (flags & 0x02) and (flags & 0x10): return 'SYN-ACK'      #start of a connection
    if (flags & 0x01) and (flags & 0x10): return 'FIN-ACK'      #end of a connection
    if (flags & 0x08) and (flags & 0x10): return 'PSH-ACK'
    if flags & 0x01: return 'FIN'
    if flags & 0x02: return 'SYN'
    if flags & 0x04: return 'RST'
    if flags & 0x08: return 'PSH'
    if flags & 0x10: return 'ACK'
    if flags & 0x20: return 'URG'

    return 'UNKNOWN'


print(f"Reading pcap file: {pcap}")
count = 0
for pkt_data, meta in RawPcapReader(pcap):
    try:
        pkt = Ether(pkt_data)
        if not pkt.haslayer(IP) or not pkt.haslayer(TCP):
            continue

        ip = pkt[IP]
        tcp = pkt[TCP]

        if (tcp.sport not in ports and tcp.dport not in ports):
            continue
        if (ip.src not in ips and ip.dst not in ips):
            continue

        entry = {
            'Timestamp': datetime.fromtimestamp(float(pkt.time)),
            'Src_IP': ip.src,
            'Dst_IP': ip.dst,
            'Src_Port': tcp.sport,
            'Dst_Port': tcp.dport,
            'Packet_Seq': tcp.seq,
            'Packet_Ack': tcp.ack,
            'Packet_Size': len(tcp.payload),
            'Packet_Flags': tcp_flags(tcp.flags),
        }

        stream.append(entry)
        count += 1

        if count % 1000 == 0:
            print(f"Processed {count} packets...")

    except Exception as e:
        print(f"Failed to parse a packet: {e}")

print(f"Total processed TCP packets: {count}")


# Convert to DataFrame and analyze
df = pd.DataFrame(stream)

df["Timestamp"] = pd.to_datetime(df["Timestamp"], unit="ms")
#print(f"Total retransmissiones: {df['Retransmission'].sum()}")  
#print(df['Timestamp'].is_monotonic_increasing)
#print(df.head(10))
#print(df.tail())
print(df.info())
print(df.describe())
df['Packet_Size'].value_counts().sort_index(ascending=True)
df['Packet_Size'].value_counts().sort_index(ascending=True)



total_packets = df['Packet_Size'].count()
total_size = df['Packet_Size'].sum()
mean_size = df['Packet_Size'].mean()
median_size = df['Packet_Size'].quantile(0.5)
#print(df['Packet_Flags'].unique())
duration = (df[df['Packet_Flags'] == 'FIN-ACK']['Timestamp'].max()) - (df[df['Packet_Flags'] == 'SYN-ACK']['Timestamp'].min())  #end - start

print(f"duration of the streaming: {duration}")
print(f"Total number of packets: {total_packets}")

print(f"Mean packet size: {mean_size:.2f} bytes")
print(f"Median packet size: {median_size:.2f} bytes")
print(f"Total size of packets: {total_size} Bytes ~ {(total_size / (1024 * 1024)):.2f} MB ~ {(total_size / (1024 * 1024 * 1024)):.2f} GB")

df.to_csv("/home/seena/pcap/{}/stream_packets.csv".format(exp), index=False)


df.plot(x="Timestamp", y="Packet_Size", title="Packet Size", kind="line")
#df.set_index("Timestamp").resample("5s")['Packet_Size'].plot(title="Packets per second", kind="line")




# Fix for Agg OverflowError
mpl.rcParams['agg.path.chunksize'] = 10000

# Filter small and large ranges
df_small = df[df['Packet_Size'] <= 100]
df_large = df[df['Packet_Size'] >= 60000]

# Create broken y-axis
fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, figsize=(14, 6), gridspec_kw={'height_ratios': [1, 2]})

# Plot each range as a separate line
ax1.plot(df_large['Timestamp'], df_large['Packet_Size'], color='red', label='Large Packets (60k+)')
ax2.plot(df_small['Timestamp'], df_small['Packet_Size'], color='blue', label='Small Packets (<=100)')

# Set y-limits to zoom in on each range
#ax1.set_ylim(60000, df['Packet_Size'].max() + 1000)
ax1.set_ylim(59500, 61500)
ax2.set_ylim(0, 100)

# Broken axis spines
ax1.spines['bottom'].set_visible(False)
ax2.spines['top'].set_visible(False)
ax1.tick_params(labeltop=False)

# Diagonal break lines
d = .015
kwargs = dict(transform=ax1.transAxes, color='k', clip_on=False)
ax1.plot((-d, +d), (-d, +d), **kwargs)
ax1.plot((1 - d, 1 + d), (-d, +d), **kwargs)

kwargs.update(transform=ax2.transAxes)
ax2.plot((-d, +d), (1 - d, 1 + d), **kwargs)
ax2.plot((1 - d, 1 + d), (1 - d, 1 + d), **kwargs)

# Labels and legend
fig.suptitle("Small and Large Packet Sizes (Broken Y-Axis)")
ax2.set_xlabel("Timestamp")
ax1.set_ylabel("Packet Size")
ax2.set_ylabel("Packet Size")
ax1.legend()
ax2.legend()

plt.tight_layout()
plt.show()



df.set_index("Timestamp").resample("5s")["Packet_Size"].count().plot(title="packet cnt per sec")            # packet count over time or activity level
df.set_index("Timestamp").resample("5s")["Packet_Size"].sum().plot(title="bytes per sec")                   # throughput approximation
