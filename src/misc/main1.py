import argparse
import threading, queue, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from globus_compute_sdk import Client 
from sci_funcs import p2cs, c2cs, conin, conout
from misc.mini_funcs import daq, dist, sirt


def get_args():
    argparser = argparse.ArgumentParser(description="arguments")
    argparser.add_argument('--sync-port', help="syncronization port",default="5000")
    argparser.add_argument('--p2cs-listener', help="listerner's IP of p2cs", default="128.135.24.119")
    argparser.add_argument('--p2cs-ip', help="IP address of the s2cs on producer side", default="128.135.164.119")
    argparser.add_argument('--type', help= "proxy type", default="StunnelSubprocess")
    argparser.add_argument('--c2cs-listener', help="listerner's IP of c2cs", default="128.135.24.120")
    argparser.add_argument('--c2cs_ip', help="IP address of the s2cs on consumer side", default='128.135.164.120')
    argparser.add_argument('--prod-ip', help="producer's IP address", default='128.135.24.117')
    #argparser.add_argument('--cons-ip', help="consumer's IP address", default="128.135.24.118")
    argparser.add_argument('--version', help="scistream version", default="1.2")

    return argparser.parse_args()

    """
    server: (Self-Signed)
    openssl req -x509 -nodes -days 365 \
    -newkey rsa:2048 \
    -keyout server.key -out server.crt \
    -subj "/CN=192.168.210.11" \
    -addext "subjectAltName=IP:172.17.0.2"
    
    or using Certificate Signing Request (CSR):
    on the server
    openssl req -new -newkey rsa:2048 -nodes \
    -keyout server.key -out server.csr \
    -subj "/CN=192.168.1.100" \
    -addext "subjectAltName=IP:192.168.1.100"
    
        Samething as above:
            openssl genrsa -out server.key 2048
            openssl req -new -key server.key -out server.csr \
            -subj "/CN=192.168.1.100" \
            -addext "subjectAltName=IP:192.168.1.100"

    client:
    # 1. Generate client.key (same as before)
    openssl genrsa -out client.key 2048

    # 2. Create a self-signed certificate for the "client CA"
    openssl req -x509 -new -key client.key -out client.crt \
    -days 365 -subj "/CN=MyLocalCA"

    # 3. Sign the CSR from the server
    openssl x509 -req -in server.csr -CA client.crt -CAkey client.key \
    -CAcreateserial -out server.crt -days 365 \
    -extfile <(printf "subjectAltName=IP:192.168.1.100")
    """
    
    
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
                #get_ep_stat(client, ep.get('uuid'), str(name))
                return ep.get('uuid')
    except Exception as e:
        print(f"error fetching {name}: {str(e)}")
    return None







if __name__ == "__main__":

    gcc = Client()
    args = get_args()

    inbounds = {"that": p2cs, "swell": conin}
    outbounds = {"neat": c2cs, "swell": conout}
    mini_funcs = {"daq": daq, "dist": dist, "sirt": sirt}    


    #scistream
    inbound, outbound = {}, {}
    results_queue = queue.Queue()   # queue to store results
    stream_uid, p2cs_sync, outbound_ports = None, None, None

    # iterate over sci_funcs (keys = endpoint names, values = functions)
    for sci_ep, func in inbounds.items():
        uuid = get_uuid(gcc, sci_ep)
        thread = threading.Thread(target=func,  args=(args, sci_ep, uuid, results_queue), daemon=True)
        inbound[thread] = sci_ep
        #threads.append(thread)
        thread.start()

    #print("\nMAIN:     Waiting for the scistream uid and outbound ports to be received")
    # make sure it gets the uuid and por brfore starting the outbound
    """while any(t.is_alive() for t in inbound):
        try:
            while stream_uid is None or outbound_ports is None:
                key, value = results_queue.get()
                if key =="uuid":
                    stream_uid = value
                    print(f"got the uid in the main: {stream_uid}")
                #elif key == "sync":
                #    p2cs_sync = value
                elif key == "ports":
                    outbound_ports = value
                    print(f"got the ports  in the main: {outbound_ports}")
        except queue.Empty:
            pass
        time.sleep(1)"""

    # check all ibound endpoints are finished
    for thread, sci_ep in inbound.items():
        thread.join()
        print(f"MAIN:     Task Execution on Endpoint '{sci_ep}' has Finished")

    print(f"\nMAIN:     Will start the outbound process with {stream_uid} and {outbound_ports}")

    """if stream_uid is None or outbound_ports is None:
        print(f"MAIN:     Error: Required values missing. Exiting: {stream_uid} and {outbound_ports}")
        exit(1)
    else:
        print(f"MAIN:     Sending the values to conout: {stream_uid} and {outbound_ports}")

    # Start Outbound Threads
    for sci_ep, func in outbounds.items():
        uuid = get_uuid(gcc, sci_ep)
        thread = threading.Thread(target=func, args=(args, sci_ep, uuid, stream_uid, outbound_ports, results_queue), daemon=True)
        outbound[thread] = sci_ep
        thread.start()

    # check all outbound endpoints are finished
    for thread, sci_ep in outbound.items():
        thread.join()
        print(f"MAIN:     Task Execution on Endpoint '{sci_ep}' has Finished")"""









    """# iterate over sci_funcs (keys = endpoint names, values = functions)
    for sci_ep, func in outbound.items():
        uuid = get_uuid(gcc, sci_ep)
        thread = threading.Thread(
            target=func, 
            args=(args, uuid, stream_uid, outbound_ports, results_queue), 
            daemon=True
        )
        outbound[thread] = sci_ep
        threads2.append(thread)"""


    """# mini-aps
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

"""