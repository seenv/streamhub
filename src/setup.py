import logging, time, sys, re
from globus_compute_sdk import Executor, ShellFunction

def p2cs(args, endpoint_name, uuid):
    """Start the Producer's S2CS on the endpoint with the given arguments."""

    cmd =   f"""
            bash -c '
            [[ -z "$HAPROXY_CONFIG_PATH" ]] && HAPROXY_CONFIG_PATH="/tmp/.scistream" && mkdir -p "$HAPROXY_CONFIG_PATH"
            if [[ -z "$(ps -ef | grep "[ ]$(cat /tmp/.scistream/s2cs.pid)")" ]]; then timeout 60s setsid stdbuf -oL -eL s2cs --server_crt=$HAPROXY_CONFIG_PATH/server.crt --server_key=$HAPROXY_CONFIG_PATH/server.key --verbose --listener_ip={args.p2cs_listener} --type="{args.type}" > "$HAPROXY_CONFIG_PATH/p2cs.log" 2>&1 & echo $! > "$HAPROXY_CONFIG_PATH/s2cs.pid"; else timeout 60s setsid stdbuf -oL -eL s2cs --server_crt=$HAPROXY_CONFIG_PATH/server.crt --server_key=$HAPROXY_CONFIG_PATH/server.key --verbose --listener_ip={args.p2cs_listener} --type="{args.type}" > "$HAPROXY_CONFIG_PATH/p2cs.log" 2>&1 & echo $! > "$HAPROXY_CONFIG_PATH/s2cs.pid"; fi
            sleep 1 && cat "$HAPROXY_CONFIG_PATH/p2cs.log"
            '
            """

    with Executor(endpoint_id=uuid) as gce:

        print(f"Starting the Producer's S2CS: \n"
              f"    endpoint: {endpoint_name.capitalize()} \n"
              f"    endpoint uid: {uuid} \n"
              f"    sync_port: {args.sync_port} \n"
              f"    p2cs_listener: {args.p2cs_listener} \n"
              f"    inbound_starter: {args.inbound_ep} \n"
              f"    type: {args.type} \n"
              f"    rate: {args.rate} \n"
              f"    num_conn: {args.num_conn} \n"
              f"\n")
        logging.debug(f"P2CS: Starting the Producer's S2CS on the endpoint ({endpoint_name.capitalize()}) with args: \n{args}")
        future = gce.submit(ShellFunction(cmd, walltime=10))

        try:
            result = future.result()
            logging.debug(f"Producer's S2CS Stdout: {result.stdout}")

        except Exception as e:
            logging.error(f"Producer's S2CS Exception: {e}")
            sys.exit(1)
            
        print(f"Producer's S2CS is completed on the endpoint {endpoint_name.capitalize()} \n") 



def c2cs(args, endpoint_name, uuid):
    """Start the Consumer's S2CS on the endpoint with the given arguments."""
    
    cmd =   f"""
            bash -c '
            [[ -z "$HAPROXY_CONFIG_PATH" ]] && HAPROXY_CONFIG_PATH="/tmp/.scistream" && mkdir -p "$HAPROXY_CONFIG_PATH"
            if [[ -z "$(ps -ef | grep "[ ]$(cat /tmp/.scistream/s2cs.pid)")" ]]; then timeout 60s setsid stdbuf -oL -eL s2cs --server_crt=$HAPROXY_CONFIG_PATH/server.crt --server_key=$HAPROXY_CONFIG_PATH/server.key --verbose --listener_ip={args.c2cs_listener} --type="{args.type}"  > "$HAPROXY_CONFIG_PATH/c2cs.log" 2>&1 & echo $! > "$HAPROXY_CONFIG_PATH/s2cs.pid"; else timeout 60s setsid stdbuf -oL -eL s2cs --server_crt=$HAPROXY_CONFIG_PATH/server.crt --server_key=$HAPROXY_CONFIG_PATH/server.key --verbose --listener_ip={args.c2cs_listener} --type="{args.type}"  > "$HAPROXY_CONFIG_PATH/c2cs.log" 2>&1 & echo $! > "$HAPROXY_CONFIG_PATH/s2cs.pid"; fi
            sleep 1 && cat "$HAPROXY_CONFIG_PATH/c2cs.log"
            '
            """

    with Executor(endpoint_id=uuid) as gce:

        print(f"Starting the Consumer's S2CS: \n"
              f"    endpoint: {endpoint_name.capitalize()} \n"
              f"    endpoint uid: {uuid} \n"
              f"    sync_port: {args.sync_port} \n"
              f"    c2cs_listener: {args.c2cs_listener} \n"
              f"    outbound_starter: {args.outbound_ep} \n"
              f"    type: {args.type} \n"
              f"    rate: {args.rate} \n"
              f"    num_conn: {args.num_conn} \n"
              f"\n")
        logging.debug(f"C2CS: Starting the Consumer's S2CS on the endpoint {endpoint_name.capitalize()} with args: \n{args}")
        future = gce.submit(ShellFunction(cmd, walltime=10))

        try:
            result = future.result()
            logging.debug(f"Consumer's S2CS Stdout: {result.stdout}")

        except Exception as e:
            logging.error(f"Consumer's S2CS Exception: {e}")
            sys.exit(1)
            
        print(f"Consumer's S2CS is completed on the endpoint {endpoint_name.capitalize()} \n")     #TODO: first check if the s2cs is online and then print this message



def inbound(args, endpoint_name,uuid, max_retries=3, delay=2):
    """Start the inbound connection and extract Stream UID and Port with retries."""

    cmd =   f"""
            bash -c '
            [[ -z "$HAPROXY_CONFIG_PATH" ]] && HAPROXY_CONFIG_PATH="/tmp/.scistream" && mkdir -p "$HAPROXY_CONFIG_PATH"
            sleep 1
            timeout 10 s2uc inbound-request --server_cert=$HAPROXY_CONFIG_PATH/server.crt --remote_ip {args.prod_ip} --num_conn {args.num_conn} --receiver_ports={args.inbound_src_ports}  --s2cs {args.p2cs_ip}:{args.sync_port}  > "$HAPROXY_CONFIG_PATH/conin.log" 2>&1 & echo $! >> "$HAPROXY_CONFIG_PATH/inbound.pid"
            while ! grep -q "prod_listeners:" "$HAPROXY_CONFIG_PATH/conin.log"; do sleep 1; done
            sleep 1
            cat "$HAPROXY_CONFIG_PATH/conin.log"
            '
            """

    with Executor(endpoint_id=uuid) as gce:

        print(f"Starting the Inbound Connection: \n"
              f"    endpoint: {endpoint_name.capitalize()} \n"
              f"    endpoint uid: {uuid} \n"
              f"    sync_port: {args.sync_port} \n"
              f"    p2cs_ip: {args.p2cs_ip} \n"
              f"    prod_ip: {args.prod_ip} \n"
              f"    inbound_src_ports: {args.inbound_src_ports} \n"
              f"\n")
        logging.debug(f"INBOUND: Starting connection on endpoint ({endpoint_name.capitalize()}) with args: \n{args}")
        future = gce.submit(ShellFunction(cmd, walltime=10))

        try:
            while not future.done():
                time.sleep(1)
                
            print("The Inbound Connection is completed.")
            result = future.result()
            logging.debug(f"INBOUND Output: {result.stdout}")
            
            info = result.stdout
            _LISTENERS = re.findall(r'(?im)^listeners:\s*"([^"]+)"', info)
            _PROD_LISTENERS = re.findall(r'(?im)^prod_listeners:\s*"([^"]+)"', info)
            _UID = re.search(r'^([a-f0-9-]{36})\s+.*INVALID_TOKEN PROD', info, re.MULTILINE)

            stream_uid = _UID.group(1) if _UID else None
            listen_ports = [port.split(":")[-1] for port in _LISTENERS]
            prod_ports = [port.split(":")[-1] for port in _PROD_LISTENERS]
            print(f"stream: {stream_uid}, listen ports: {listen_ports}, and the prod ports: {prod_ports}")
            
            return stream_uid, listen_ports

        except Exception as e:
            logging.error(f"INBOUND Exception: {e}")
            print(f"The Inbound Connection failed due to the following Exception: {e} \n")
            sys.exit(1)

        logging.error("INBOUND: The Inbound Connection failed to extract Stream UID and Port.")
        print(f"The Inbound Connection failed to extract Stream UID and Port. \n")
        
        
        
def outbound(args, endpoint_name, uuid, stream_uid, ports):
    """Start the outbound connection using the extracted Stream UID and Port."""

    if not stream_uid or len(ports) < int(args.num_conn):
        print(f"The Outbound Connection failed due to missing Stream UID or Port on the endpoint {endpoint_name.capitalize()} {stream_uid, ports}")
        logging.error(f"OUTBOUND: The Outbound Connection failed due to missing Stream UID or Port on the endpoint {endpoint_name.capitalize()} {stream_uid, ports}")
        return

    cmd =   f"""
            bash -c '
            [[ -z "$HAPROXY_CONFIG_PATH" ]] && HAPROXY_CONFIG_PATH="/tmp/.scistream" && mkdir -p "$HAPROXY_CONFIG_PATH"
            timeout 10s setsid stdbuf -oL -eL s2uc outbound-request --server_cert=$HAPROXY_CONFIG_PATH/server.crt --remote_ip {args.c2cs_ip} --num_conn {args.num_conn} --s2cs {args.c2cs_ip}:{args.sync_port}  --receiver_ports={args.outbound_dst_ports} "{stream_uid}" {args.p2cs_ip}:5100,{args.p2cs_ip}:5101,{args.p2cs_ip}:5102,{args.p2cs_ip}:5103,{args.p2cs_ip}:5104,{args.p2cs_ip}:5105,{args.p2cs_ip}:5106,{args.p2cs_ip}:5107,{args.p2cs_ip}:5108,{args.p2cs_ip}:5109,{args.p2cs_ip}:5110  > "$HAPROXY_CONFIG_PATH/conout.log" & echo $! >> "$HAPROXY_CONFIG_PATH/outbound.pid"
            while ! grep -q "Hello message sent successfully" "$HAPROXY_CONFIG_PATH/conout.log"; do sleep 1 ; done
            sleep 1
            cat "$HAPROXY_CONFIG_PATH/conout.log"
            '
            """

    with Executor(endpoint_id=uuid) as gce:

        print(f"Starting the Outbound Connection: \n"
              f"    endpoint: {endpoint_name.capitalize()} \n"
              f"    endpoint uid: {uuid} \n"
              f"    p2cs_listener: {args.p2cs_listener} \n"
              f"    c2cs_listener: {args.c2cs_listener} \n"
              f"    p2cs_ip: {args.p2cs_ip} \n"
              f"    cons_ip: {args.cons_ip} \n"
              f"\n")
        logging.debug(f"OUTBOUND: Starting Outbound connection on endpoint ({endpoint_name.capitalize()}) with args: \n{args} \n")
        future = gce.submit(ShellFunction(cmd, walltime=10))

        try:
            result = future.result()
            logging.debug(f"OUTBOUND Output: {result.stdout}")

        except Exception as e:
            logging.error(f"OUTBOUND Exception: {e}")
            print(f"The Outbound Connection failed due to the following Exception: {e}")
            sys.exit(1)

        print(f"The Outbound Connection is completed on the endpoint {endpoint_name.capitalize()} \n")
        logging.debug(f"OUTBOUND: The Outbound Connection is completed on the endpoint {endpoint_name.capitalize()}")