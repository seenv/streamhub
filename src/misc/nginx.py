from datetime import datetime
import logging, time, sys, re
from globus_compute_sdk import Executor, ShellFunction
from config import get_args



def p2cs_nginx_conf(args, endpoint_name, uuid):
    """ Configure Nginx for the Stream Compute Service. """
    
    #args = get_args()
    #uuid = get_uuid(args.p2cs_ep)
    
    cmd = f"""bash -c '
    NGINX_CONFIG_PATH="${{{{NGINX_CONFIG_PATH:-/tmp/.scistream}}}}"
    mkdir -p "$NGINX_CONFIG_PATH"
    P2CS_LISTEN=5100 && PROD_LISTEN=5074
    echo -e "user nginx; \\nworker_processes auto; \\n \\nerror_log $NGINX_CONFIG_PATH/error.log notice; \\npid $NGINX_CONFIG_PATH/nginx.pid; \\n \\nevents {{{{\\n  worker_connections  1024; \\n}}}} \\n \\nstream {{{{\\n$(for p in {{{{0..{((args.num_conn -1))}}}}}; do echo -e \\  server {{{{\\\\n \\     listen $((P2CS_LISTEN + p))\;\\\\n \\     proxy_pass {args.prod_ip}:$((PROD_LISTEN + p))\\; \\\\n \\ }}}}; done)\\n}}}}" > "$NGINX_CONFIG_PATH/nginx.conf"
    echo "[INFO] Generated: $NGINX_CONFIG_PATH/nginx.conf"
    sudo nginx -c "$NGINX_CONFIG_PATH/nginx.conf"
    '"""
    #sudo nginx -s stop -c "$PWD/nginx.conf"
    
    with Executor(endpoint_id=uuid) as gce:
        print(f"Starting NGINX on Producer's S2CS: \n"
            f"    endpoint: {endpoint_name.upper()} \n"
            f"    endpoint uid: {uuid} \n"
            f"    port: {args.inbound_src_ports} \n"
            f"    prod_ip: {args.prod_ip} \n"
            f"    type: {args.type} \n"
            f"\n")
        logging.info(f"P2CS: Starting the Nginx on S2CS with args: \n{args}")
        future = gce.submit(ShellFunction(cmd, walltime=10))
        
        try:
            result = future.result()
            logging.info(f"P2CS: Nginx Stdout: {result.stdout}")
            logging.debug(f"Nginx Stderr: {result.stderr}")
            #logging.info(f"Nginx on the endpoint {endpoint_name.upper()} Stdout: \n{result.stdout} \n")
            
        except Exception as e:
            logging.error(f"P2CS: Nginx Exception \n{e}")
            raise RuntimeError(f"Failed to start Nginx on the endpoint {endpoint_name.upper()}: {e}")



def c2cs_nginx_conf(args, endpoint_name, uuid):
    """ Configure Nginx for the Stream Compute Service. """
    
    cmd = f"""bash -c '
    NGINX_CONFIG_PATH="${{{{NGINX_CONFIG_PATH:-/tmp/.scistream}}}}" && mkdir -p "$NGINX_CONFIG_PATH" 
    P2CS_LISTEN=5100 && C2CS_LISTEN=5100
    echo -e "user nginx; \\nworker_processes auto; \\n \\nerror_log $NGINX_CONFIG_PATH/error.log notice; \\npid $NGINX_CONFIG_PATH/nginx.pid; \\n \\nevents {{{{\\n  worker_connections  1024; \\n}}}} \\n \\nstream {{{{\\n$(for p in {{{{0..{((args.num_conn -1))}}}}}; do echo -e \\  server {{{{\\\\n \\     listen $((C2CS_LISTEN + p))\;\\\\n \\     proxy_pass {args.p2cs_ip}:$((P2CS_LISTEN + p))\\; \\\\n \\ }}}}; done)\\n}}}}" > "$NGINX_CONFIG_PATH/nginx.conf"
    echo "[INFO] Generated: $NGINX_CONFIG_PATH/nginx.conf"
    sleep 5 && sudo nginx -c "$NGINX_CONFIG_PATH/nginx.conf"
    '"""
    
    
    with Executor(endpoint_id=uuid) as gce:
        print(f"Starting NGINX on the Consumer's S2CS: \n"
            f"    endpoint: {endpoint_name.upper()} \n"
            f"    endpoint uid: {uuid} \n"
            f"    port: {args.outbound_dst_ports} \n"
            f"    p2cs_ep: {args.p2cs_ep} \n"
            f"    type: {args.type} \n"
            f"\n")
        logging.info(f"P2CS: Starting Nginx on S2CS with args: \n{args}")
        future = gce.submit(ShellFunction(cmd, walltime=10))
        
        try:
            result = future.result()
            logging.info(f"P2CS: Nginx Stdout: {result.stdout}")
            #logging.debug(f"Nginx Stderr: {result.stderr}")
            #logging.info(f"Nginx on the endpoint {endpoint_name.upper()} Stdout: \n{result.stdout} \n")
            
        except Exception as e:
            logging.error(f"P2CS: Nginx Exception \n{e}")
            raise RuntimeError(f"Failed to start Nginx on the endpoint {endpoint_name.upper()}: {e}")




"""if __name__ == "__main__":
    args = get_args()
    endpoint_name = "swell"
    uuid = "efcb05ba-47aa-4c22-832f-21f3e23530a6"
    p2cs_nginx_conf(args, endpoint_name, uuid)
    print("Nginx configuration completed.")"""
    