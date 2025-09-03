import time
from globus_compute_sdk import Executor, Client, ShellFunction
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime


def run_executor(endpoint_id, bf, code):
    import time
    from datetime import datetime
    #print(f"start on {endpoint_id} with: {code}")
    with Executor(endpoint_id=endpoint_id) as gce:
        print(f"execute on {endpoint_id}")
        future = gce.submit(bf, timeout=20)
        #shell_result = future.result()
        #print(f"DDDDBBBBGGGG: endpoint {endpoint_id}: {shell_result.stdout}")
        #shell_result = future.result()
        #elapsed_time = time.time() - start_time
        #time.sleep(10)
        return future
        

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


gcc = Client()

#endpoints = {"pub": "swell-guy", "sub": "this-guy"}
#endpoints = {"pub": "this", "this": "pub", "p2cs": "that", "c2cs": "neat", "con": "swell", "con": "swell"}
endpoints = {"pub": "this"}
#ep_ips = {"this": "128.135.24.117", "swell": "128.135.24.118", "that":"128.135.164.119", "neat": "128.135.164.120"}
endpoint_ids = {key: get_uuid(gcc, name) for key, name in endpoints.items()}
"""
commands = {"p2cs": "s2cs --verbose --port=5007 --listener-ip=128.135.24.119 --type=Haproxy",
            "pub": "s2uc prod-req --s2cs 128.135.24.119:5007 --mock True &",
            "pub": "appctrl mock 4f8583bc-a4d3-11ee-9fd6-034d1fcbd7c3 128.135.24.119:5007 INVALID_TOKEN PROD 128.135.24.117",
            "c2cs": "s2cs --verbose --port=5007 --listener-ip=128.135.24.120 --type=Haproxy",
            "con": "s2uc cons-req --s2cs 128.135.24.120:5007 4f8583bc-a4d3-11ee-9fd6-034d1fcbd7c3 128.135.164.120:5074 &",
            "con": "appctrl mock 4f8583bc-a4d3-11ee-9fd6-034d1fcbd7c3 128.135.24.120:5007 INVALID_TOKEN PROD 128.135.164.120"}
"""
commands = {"pub": "cd /home/seena/globus-stream/scistream-docker && docker-compose up"}


shell_functions = {key: ShellFunction(cmd) for key, cmd in commands.items()}

with ThreadPoolExecutor(max_workers=len(endpoints)) as executor:
    future_to_endpoint = {
        executor.submit(run_executor, endpoint_ids[key], shell_func, commands[key]): key
        for key, shell_func in shell_functions.items()}

print("\n")

for future in as_completed(future_to_endpoint):
    endpoint_name = future_to_endpoint[future]
    try:
        result_future = future.result() 
        result = result_future.result() 
        print(f"Task completed for endpoint {endpoint_name}:")
        print(f"Stdout: {result.stdout}")
        print(f"Stderr: {result.stderr}")
    except Exception as e:
        print(f"Task failed for endpoint {endpoint_name}: {e}")
