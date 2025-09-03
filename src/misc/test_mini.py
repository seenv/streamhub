import argparse
import threading, queue, time, logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from globus_compute_sdk import Client 
from setup import p2cs, c2cs, inbound, outbound, kill_orphans
from misc.mini_funcs import daq, dist, sirt


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("mini.log"), 
        #logging.StreamHandler()
    ]
)



def get_args():
    argparser = argparse.ArgumentParser(description="arguments")
    argparser.add_argument('--sync_port', help="syncronization port",default="5000")
    argparser.add_argument('--p2cs_listener', help="listerner's IP of p2cs", default="128.135.24.119")
    argparser.add_argument('--p2cs_ip', help="IP address of the s2cs on producer side", default="128.135.164.119")
    argparser.add_argument('--c2cs_listener', help="listerner's IP of c2cs", default="128.135.24.120")
    argparser.add_argument('--c2cs_ip', help="IP address of the s2cs on consumer side", default='128.135.164.120')
    argparser.add_argument('--prod_ip', help="producer's IP address", default='128.135.24.117')
    argparser.add_argument('--cons_ip', help="consumer's IP address", default="128.135.24.118")
    argparser.add_argument('--inbound_starter', help="initiate the inbound stream connection", default="swell")             # the server certification should be specified
    argparser.add_argument('--outbound_starter', help="initiate the outbound stream connection", default="swell")           # the server certification should be specified
    #argparser.add_argument('-v', '--verbose', action="store_true", help="Initiate a new stream connection", default=False)

    argparser.add_argument('--type', help= "proxy type: HaproxySubprocess, StunnelSubprocess", default="StunnelSubprocess")
    argparser.add_argument('--rate', type=int, help="transfer rate",default=10000)                                          #TODO: Add it to the command lines
    argparser.add_argument('--num_conn', type=int, help="THe number of specified ports", default=5)
    argparser.add_argument('--inbound_src_ports', type=str, help="Comma-separated list of inbound receiver ports", default="5074,5075,5076,5077,5078")
    argparser.add_argument('--outbound_dst_ports', type=str, help="Comma-separated list of outbound receiver ports", default="5100,5101,5102,5103,5104")    #dynamically is increased by the s2uc and then is read from the log file

    argparser.add_argument('--num_mini', type=int, help="The number of concurrent aps-mini-app transaction run", default=1)

    
    return argparser.parse_args()



def get_status(gcc, uuid, name):
    """Get the status of the endpoint with the given endpoint UUID."""

    status = get_endpoint_status(uuid)
    metadata = gcc.get_endpoint_metadata(uuid)
    stop_response = gcc.stop_endpoint(uuid)            #Return type: json
    task_id = gcc.get_worker_hardware_details(uuid)
    result_status = gcc.get_result(task_id)



def get_uuid(client, name):
    """Get the UUID of the endpoint with the given endpoint."""

    try:
        endpoints = client.get_endpoints()
        for ep in endpoints:
            endpoint_name = ep.get('name', '').strip()
            if endpoint_name == name.strip().lower():
                #get_status(client, ep.get('uuid'), str(name))
                return ep.get('uuid')
    except Exception as e:
        logging.debug(f"error fetching {name}: {str(e)}")
    return None



def start_s2cs(args, gcc, s2cs):
    """Start the S2CS functions "p2cs" and "c2cs" on the producer and consumer endpoints."""

    s2cs_threads = {}

    # iterate over sci_funcs (keys = endpoint names, values = functions)
    for s2cs_endpoint, func in s2cs.items():
        thread = threading.Thread(target=func,  args=(args, s2cs_endpoint, get_uuid(gcc, s2cs_endpoint)), daemon=True)
        s2cs_threads[thread] = s2cs_endpoint
        thread.start()
        logging.debug(f"MAIN: The S2CS '{s2cs_endpoint}' has started")

    for thread, s2cs_endpoint in s2cs_threads.items():
        thread.join()
        logging.debug(f"MAIN: The S2CS '{s2cs_endpoint}' has finished")



def start_connection(args, gcc, connections):
    """Manage the full connection process, optionally running inbound and outbound in parallel."""

    connections = {args.inbound_starter: inbound, args.outbound_starter: outbound}

    stream_uid, ports = inbound(args, args.inbound_starter,  get_uuid(gcc, args.inbound_starter))
    if stream_uid and len(ports) == int(args.num_conn):
        outbound(args, args.inbound_starter, get_uuid(gcc, args.outbound_starter), stream_uid, ports) 
    else:
        logging.error("Failed to retrieve Stream UID and Port. Outbound will not start.")
        #exit(1)
    


def cleaning_s2cs():
    """
    Kill the orphan processes of the S2CS on the producer and consumer endpoints.
    Since the processes are disowned using the 'setsid', they need to be killed
    no matter the connection status. (it won't affect the status)
    """

    s2cs_threads = {}

    # iterate over sci_funcs (keys = endpoint names, values = functions)
    for s2cs_endpoint, _ in s2cs.items():
        thread = threading.Thread(target=kill_orphans,  args=(args, s2cs_endpoint, get_uuid(gcc, s2cs_endpoint)), daemon=True)
        s2cs_threads[thread] = s2cs_endpoint
        thread.start()
        logging.debug(f"MAIN: Starting killing Orphan processes on '{s2cs_endpoint}' ")
    
    for thread, s2cs_endpoint in s2cs_threads.items():
        thread.join()
        logging.debug(f"MAIN: Finished killing Orphan processes on '{s2cs_endpoint}' ")



def start_mini():
    """Starts the mini functions "daq", "dist" on the producer and "sirt" consumer endpoints."""

    mini = {}

    for mini_endpoint, func in mini_funcs.items():
        uuids = get_uuid(gcc, mini_endpoint)
        thread = threading.Thread(target=func, args= (args, uuids), daemon=True)
        mini[thread] = mini_endpoint
    
    for thread in mini:
        thread.start()

    for thread in mini:
        thread.join()
        print(f"Task Execution on Endpoint '{mini[thread]}' has Finished")



gcc = Client()
args = get_args()
s2cs = {"that": p2cs, "neat": c2cs}
connections = {args.inbound_starter: inbound, args.outbound_starter: outbound}
mini_funcs = {"daq": daq, "dist": dist, "sirt": sirt} 

if __name__ == "__main__":
        
    #start_s2cs(args, gcc, s2cs)
    #start_connection(args, gcc, connections)
    #cleaning_s2cs()
    start_mini()
