import logging, time, sys
from globus_compute_sdk import Executor, ShellFunction

#TODO: add the correct log and also kill the processes correctly

def pdump(args, endpoint_name , uuid):
    """Killing the orphaned processes initiated via globus worker"""
    #TODO: now that we have the uuid we can just kill the pid as they are the same! or can we?!

    cmd =   f"""
            bash -c '
            mkdir -p "{pcap_dir}"
            timeout 60s setsid stdbuf -oL -eL sudo dumpcap -i eno1np0 -s 96 -f "host 129.114.108.216" -w "{pcap_dir}/p2stream.pcapng"
            '
            """
    
    with Executor(endpoint_id=uuid) as gce:
        
        print(f"started the capture on p2cs: \n"
              f"    endpoint: {endpoint_name.capitalize()} \n"
              f"    endpoint uid: {uuid} \n"
              f"\n")
        logging.debug(f"KILL_ORPHANS: Killing orphaned processes on endpoint ({endpoint_name.capitalize()}) with args: \n{args}")
        future = gce.submit(ShellFunction(cmd))

        try:
            result = future.result()
            logging.debug(f"KILL_ORPHANS Output: {result.stdout}")
            #logging.debug(f"KILL_ORPHANS Errors: {result.stderr}")

        except Exception as e:
            logging.error(f"KILL_ORPHANS Exception: {e}")
            print(f"Killing the orphaned processes failed due to the following Exception: {e}")
            sys.exit(1)

        print(f"Killing the orphaned processes is completed on the endpoint {endpoint_name.capitalize()} \n")
        logging.debug(f"KILL_ORPHANS: Killing orphaned processes is completed on the endpoint {endpoint_name.capitalize()}")
        #gce.shutdown(wait=True, cancel_futures=False)
        
        