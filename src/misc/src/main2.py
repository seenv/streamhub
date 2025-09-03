import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from globus_compute_sdk import Client 
from sci_funcs import p2cs, c2cs, pub, con
from misc.mini_funcs import daq, dist, sirt


def get_args():
    argparser = argparse.ArgumentParser(description="arguments")
    argparser.add_argument('--sync-port', help="syncronization port",default="5007")
    argparser.add_argument('--p2cs-listener', help="listerner's IP of p2cs", default="128.135.24.119")
    argparser.add_argument('--p2cs-ip', help="IP address of the s2cs on producer side", default="128.135.164.119")
    argparser.add_argument('--type', help= "proxy type", default="Haproxy")
    argparser.add_argument('--c2cs-listener', help="listerner's IP of c2cs", default="128.135.24.120")
    argparser.add_argument('--c2cs_ip', help="IP address of the s2cs on consumer side", default='128.135.164.120')
    argparser.add_argument('--prod-ip', help="producer's IP address", default='128.135.24.117')
    #argparser.add_argument('--cons-ip', help="consumer's IP address", default="128.135.24.118")

    return argparser.parse_args()


def get_ep_stat(gcc, uuid, name):
    from globus_compute_sdk import Executor, ShellFunction, Client

    command = "globus-compute-endpoint list "
    shell_function = ShellFunction(command, walltime=30)
    with Executor(endpoint_id=uuid)as gce:
        future = gce.submit(shell_function)
    try:
        result = future.result(timeout=10)
        print(f"Endpoint {name.capitalize()} Status: \n{result.stdout}", flush=True)
        cln_stderr = "\n".join(line for line in result.stderr.split("\n") if "WARNING" not in line)
        if cln_stderr.strip():
            print(f"Stderr: {cln_stderr}", flush=True)
    except Exception as e:
        print(f"Getting EP Status failed: {e}")



def get_uuid(client, name):
    try:
        endpoints = client.get_endpoints()
        for ep in endpoints:
            endpoint_name = ep.get('name', '').strip()
            if endpoint_name == name.strip().lower():
                #print(f"DEBUG:EndPoint: {name} with UUID: {ep.get('uuid')}")
                get_ep_stat(client, ep.get('uuid'), str(name))
                return ep.get('uuid')
    except Exception as e:
        print(f"error fetching {name}: {str(e)}")
    return None





if __name__ == "__main__":

    gcc = Client()
    args = get_args()

    mini_funcs = {"dist": dist, "sirt": sirt}

    # mini-aps
    mini = {}
    for mini_ep, func in mini_funcs.items():
        uuids = get_uuid(gcc, mini_ep)
        thread = threading.Thread(target=func, args= (args, uuids), daemon=True)
        mini[thread] = mini_ep
    
    for thread in mini:
        thread.start()

    for thread in mini:
        thread.join()
        print(f"Task Execution on Endpoint '{mini[thread]}' has Finished")