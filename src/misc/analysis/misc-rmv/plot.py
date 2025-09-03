import pandas as pd
import matplotlib.pyplot as plt

#df = pd.read_csv("this/test_1/flow_stats_fixed.csv", parse_dates=["Start_Time"])
#df = pd.read_csv("this/test_2/flow_stats_fixed.csv", parse_dates=["Start_Time"])
#df = pd.read_csv("swell/test_1/flow_stats_fixed.csv", parse_dates=["Start_Time"])
df = pd.read_csv("swell/test_2/flow_stats_fixed.csv", parse_dates=["Start_Time"])

#output_folder = "this/test_1/plots"
#output_folder = "this/test_2/plots"
#output_folder = "swell/test_1/plots"
output_folder = "swell/test_2/plots"

df_tcp = df[df["Protocol"] == "TCP"].copy()

df_tcp.rename(columns={"Flow_Duration (sec)": "Flow_Duration"}, inplace=True)

df_tcp = df_tcp.sort_values(by="Start_Time")


import os
os.makedirs(output_folder, exist_ok=True)

fig, axes = plt.subplots(2, 2, figsize=(14, 10))

axes[0, 0].plot(df_tcp["Start_Time"], df_tcp["Throughput (Bytes/sec)"], marker='o', linestyle='-')
axes[0, 0].set_xlabel("Time")
axes[0, 0].set_ylabel("Throughput (Bytes/sec)")
axes[0, 0].set_title("Throughput Over Time (TCP Packets)")
axes[0, 0].tick_params(axis='x', rotation=45)
axes[0, 0].grid()
plt.savefig(f"{output_folder}/throughput.png")  

axes[0, 1].plot(df_tcp["Start_Time"], df_tcp["Flow_Duration"], marker='o', linestyle='-')
axes[0, 1].set_xlabel("Time")
axes[0, 1].set_ylabel("Latency (sec)")
axes[0, 1].set_title("Latency Over Time (TCP Packets)")
axes[0, 1].tick_params(axis='x', rotation=45)
axes[0, 1].grid()
plt.savefig(f"{output_folder}/latency.png")  

axes[1, 0].plot(df_tcp["Start_Time"], df_tcp["Packet_Count"], marker='o', linestyle='-')
axes[1, 0].set_xlabel("Time")
axes[1, 0].set_ylabel("Packet Count")
axes[1, 0].set_title("Packet Count Over Time (TCP Packets)")
axes[1, 0].tick_params(axis='x', rotation=45)
axes[1, 0].grid()
plt.savefig(f"{output_folder}/packet_count.png") 

axes[1, 1].plot(df_tcp["Start_Time"], df_tcp["Avg_Packet_Size"], marker='o', linestyle='-')
axes[1, 1].set_xlabel("Time")
axes[1, 1].set_ylabel("Average Packet Size (Bytes)")
axes[1, 1].set_title("Average Packet Size Over Time (TCP Packets)")
axes[1, 1].tick_params(axis='x', rotation=45)
axes[1, 1].grid()
plt.savefig(f"{output_folder}/avg_packet_size.png")

plt.tight_layout()

plt.savefig(f"{output_folder}/all_plots.png")

plt.show()

print(f"✅ Plots saved in {output_folder}")