import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from globus_compute_sdk import Client 
from p2cs import p2cs
from c2cs import c2cs
from pub import pub
from con import con


def get_args():
    argparser = argparse.ArgumentParser(description="arguments")
    argparser.add_argument('--sync-port', help="syncronization port",default="5007")
    argparser.add_argument('--p2cs-listener', help="listerner's IP of p2cs", default="128.135.24.119")
    argparser.add_argument('--p2cs-ip', help="IP address of the s2cs on producer side", default="128.135.164.119")
    argparser.add_argument('--type', help= "proxy type", default="Haproxy")
    argparser.add_argument('--c2cs-listener', help="listerner's IP of c2cs", default="128.135.24.120")
    argparser.add_argument('--c2cs', help="IP address of the s2cs on consumer side", default='128.135.164.120')
    argparser.add_argument('--prod-ip', help="producer's IP address", default='128.135.25.117')
    #argparser.add_argument('--cons-ip', help="consumer's IP address", default="128.135.24.118")

    return argparser.parse_args()


def get_uuid(client, name):
    try:
        endpoints = client.get_endpoints()
        for ep in endpoints:
            endpoint_name = ep.get('name', '').strip()
            if endpoint_name == name.strip().lower():
                print(f"DEBUG:EndPoint: {name} with UUID: {ep.get('uuid')}")
                return ep.get('uuid')
    except Exception as e:
        print(f"error fetching {name}: {str(e)}")
    return None


if __name__ == "__main__":

    gcc = Client()
    args = get_args()
    
    #eps = {"p2cs": "that", "c2cs": "neat", "prod": "this", "cons": "swell"}
    #ep_funcs = {"that": p2cs, "neat": c2cs, "this": pub, "swell": con}
    #ep_ids = {role: get_uuid(gcc, name) for role, name in ep_funcs.items()}

    ep_funcs = {"that": p2cs, "neat": c2cs, "this": pub, "swell": con}

    threads = {}
    # iterate directly over ep_funcs (keys = endpoint names, values = functions)
    for ep_name, func in ep_funcs.items():
        uuid = get_uuid(gcc, ep_name)
        thread = threading.Thread(target=func, args=(args, uuid), daemon=True)
        threads[thread] = ep_name


    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
        print(f"the {threads[thread]}'s is done")  













"""if __name__ == "__main__":
    endpoint_functions = {"pub": pub, "p2cs": p2cs, "c2cs": c2cs, "con": con}
    log_files = {"p2cs": "p2cs_output.log", "c2cs": "c2cs_output.log", "pub": "pub_output.log", "con": "con_output.log"}

    endpoints = ["pub", "p2cs", "c2cs", "con"]

    with ThreadPoolExecutor(max_workers=len(endpoints)) as executor:
        futures = {executor.submit(endpoint_functions[role]): role for role in endpoints}

        log_threads = [threading.Thread(target=tail_log, args=(log_files[role],), daemon=True) for role in endpoints]
        for t in log_threads:
            t.start()

    for future in as_completed(futures):
        role = futures[future]
        try:
            future.result()
            print(f"Task for {role} completed successfully.")
        except Exception as e:
            print(f'Task for {role} failed: {e}')
"""