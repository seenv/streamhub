import time
from globus_compute_sdk import Executor, Client, ShellFunction
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import sys, socket

gcc = Client()


def get_uuid(client, name):
    try:
        endpoints = client.get_endpoints()
        for ep in endpoints:
            endpoint_name = ep.get('name', '').strip()
            if endpoint_name == name.strip().lower():
                #print(f"\nfound {name}\n")
                return ep.get('uuid')
    except Exception as e:
        print(f"error fetching {name}: {str(e)}")
    return None
    
    
endpoints = {"pub": "this", "p2cs": "that", "c2cs": "neat", "con": "swell"}
ep_ips = {"this": "128.135.24.117", "swell": "128.135.24.118", "that": "128.135.164.119", "neat": "128.135.164.120"}
endpoint_ids = {key: get_uuid(gcc, name) for key, name in endpoints.items()}

commands = {"p2cs": "s2cs --verbose --port=5007 --listener-ip=128.135.24.119 --type=Haproxy"}

endpoint_id = get_uuid(gcc, "that")

enpoint = get_uuid(gcc, "p2cs")
shell_function = ShellFunction(commands["p2cs"])

with Executor(endpoint_id=endpoint_id) as gce:
    print(f"Executing on endpoint {endpoint_id}...")
    future = gce.submit(shell_function)
    print(f"Task submitted to endpoint {endpoint_id} with Task ID: {future.task_id}")

# Track the task asynchronously
print("Waiting for task completion...\n")
future_to_endpoint = {future: "p2cs"}

for future in as_completed(future_to_endpoint):
    endpoint_name = future_to_endpoint[future]
    try:
        result = future.result()
        print(f"Task completed for endpoint {endpoint_name}:")
        print(f"Stdout: {result.stdout}")
        print(f"Stderr: {result.stderr}")
    except Exception as e:
        print(f"Task failed for endpoint {endpoint_name}: {e}")