"""import time
from globus_compute_sdk import Executor, Client, ShellFunction
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
"""


import time
from globus_compute_sdk import Executor, Client
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import sys, socket, os

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

def get_venv():
    import sys, os, socket
    return {
        "sys.prefix": sys.prefix,
        "sys.base_prefix": sys.base_prefix,
        "virtual_env": sys.prefix != sys.base_prefix
    }

endpoints = ["this", "that", "swell", "neat"]

endpoint_ids = {name: get_uuid(gcc, name) for name in endpoints}

for key, endpoint_id in endpoint_ids.items():
    if endpoint_id:
        with Executor(endpoint_id=endpoint_id) as gce:
            future = gce.submit(get_venv)
            print(f"Result from {key} ({endpoint_id}): {future.result()}")
    else:
        print(f"Skipping {key}, endpoint not found.")