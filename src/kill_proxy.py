import warnings
import logging, sys
from globus_compute_sdk import Executor, ShellFunction


def stop_s2cs(args, endpoint_name , uuid):
    warnings.filterwarnings("ignore", category=UserWarning)
    """Killing the orphaned processes initiated via globus worker"""
    
    cmd =   f"""
            bash -c '
            [[ -z "$HAPROXY_CONFIG_PATH" ]] && HAPROXY_CONFIG_PATH="/tmp/.scistream" && mkdir -p "$HAPROXY_CONFIG_PATH"
            sudo pkill haproxy && sleep 1 && echo "$(ps -ef | grep haproxy )" >> "$HAPROXY_CONFIG_PATH/kill.log"
            sudo pkill stunnel && sleep 1 && echo "$(ps -ef | grep stunnel )" >> "$HAPROXY_CONFIG_PATH/kill.log"
            sudo pkill nginx && sleep 1 && echo "$(ps -ef | grep nginx)" >> "$HAPROXY_CONFIG_PATH/kill.log"
            sleep 5
            '
            """
            
    with Executor(endpoint_id=uuid) as gce:
        
        print(f"Killing the orphaned processes: \n"
              f"    endpoint: {endpoint_name.upper()} \n"
              f"    endpoint uid: {uuid} \n"
              f"\n")
        logging.debug(f"KILL_PROXY: Killing orphaned processes on endpoint ({endpoint_name.upper()}) with args: \n{args}")
        future = gce.submit(ShellFunction(cmd))

        try:
            result = future.result()
            cleaned_stderr = '\n'.join(
                line for line in result.stderr.splitlines()
                if "sandboxing" not in line.lower()
            )
            
            logging.debug(f"KILL_PROXY Output: {result.stdout}")
            if cleaned_stderr.strip():
                logging.error(f"KILL_PROXY Errors: {cleaned_stderr}")
                print(f"Killing the orphaned processes failed with the following errors: {cleaned_stderr}")

        except Exception as e:
            logging.error(f"KILL_PROXY Exception: {e}")
            print(f"Killing the orphaned processes failed due to the following Exception: {e}")
            raise RuntimeError(f"Killing the orphaned processes failed due to the following Exception: {e}")

        print(f"Killing the orphaned processes is completed on the endpoint {endpoint_name.upper()} \n")
        logging.debug(f"KILL_PROXY: Killing orphaned processes is completed on the endpoint {endpoint_name.upper()}")
        #gce.shutdown(wait=True, cancel_futures=False)
        
        
        
def stop_s2uc(args, endpoint_name , uuid):
    """Killing the orphaned processes initiated via globus worker"""

    cmd =   f"""
            bash -c '
            [[ -z "$HAPROXY_CONFIG_PATH" ]] && HAPROXY_CONFIG_PATH="/tmp/.scistream" && mkdir -p "$HAPROXY_CONFIG_PATH"
            [[ -f $HAPROXY_CONFIG_PATH/inbound.pid ]] && pid=$(cat $HAPROXY_CONFIG_PATH/inbound.pid) && [[ "$pid" =~ ^[0-9]+$ ]] && kill -9 "$pid" > "$HAPROXY_CONFIG_PATH/kill.log" 2>&1
            [[ -f $HAPROXY_CONFIG_PATH/outbound.pid ]] && pid=$(cat $HAPROXY_CONFIG_PATH/outbound.pid) && [[ "$pid" =~ ^[0-9]+$ ]] && kill -9 "$pid" >> "$HAPROXY_CONFIG_PATH/kill.log" 2>&1
            find "$HAPROXY_CONFIG_PATH" ! -name "kill.log" -delete
            '
            """
        
    with Executor(endpoint_id=uuid) as gce:
        
        print(f"Killing the orphaned processes: \n"
              f"    endpoint: {endpoint_name.upper()} \n"
              f"    endpoint uid: {uuid} \n"
              f"\n")
        logging.debug(f"KILL_PROXY: Killing orphaned processes on endpoint ({endpoint_name.upper()}) with args: \n{args}")
        future = gce.submit(ShellFunction(cmd))

        try:
            result = future.result()
            logging.debug(f"KILL_PROXY Output: {result.stdout}")

        except Exception as e:
            logging.error(f"KILL_PROXY Exception: {e}")
            print(f"Killing the orphaned processes failed due to the following Exception: {e}")
            sys.exit(1)

        print(f"Killing the orphaned processes is completed on the endpoint {endpoint_name.upper()} \n")
        logging.debug(f"KILL_PROXY: Killing orphaned processes is completed on the endpoint {endpoint_name.upper()}")
        #gce.shutdown(wait=True, cancel_futures=False)        
        
        