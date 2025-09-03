



def p2cs(args, endpoint_name, uuid, result_q):

    from globus_compute_sdk import Executor, ShellFunction, Client
    from globus_compute_sdk.sdk.executor import ComputeFuture
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    import signal
    from datetime import datetime
    import sys, socket, time, os
    import queue, ast, re

    

    command =   f"""
                bash -c '
                if [[ -n "$HAPROXY_CONFIG_PATH" ]]; then
                    CONFIG_PATH="$HAPROXY_CONFIG_PATH" && mkdir -p "$CONFIG_PATH"
                else 
                    CONFIG_PATH="/tmp/.scistream" && mkdir -p "$CONFIG_PATH"
                fi
                if [[ -s "$CONFIG_PATH/resource.map" ]]; then
                    rm -f "$CONFIG_PATH/resource.map"
                fi
                stdbuf -oL -eL s2cs --server_crt="$CONFIG_PATH/server.crt" --server_key="$CONFIG_PATH/server.key" --verbose  --listener_ip={args.p2cs_listener} --type={args.type}  > "$CONFIG_PATH/p2cs.log"  &
                while [[ ! -f "$CONFIG_PATH/resource.map" ]] || ! grep -q "Prod Listeners:" "$CONFIG_PATH/resource.map"; do
                    sleep 1
                done
                echo "P2CS:      Found the resource map file and the Prod Listeners in it"
                cat "$CONFIG_PATH/resource.map"
                sleep 10 '
                """
                #the correct command: s2cs --server_crt=/home/seena/scistream/server.crt --server_key=/home/seena/scistream/server.key --verbose  --listener_ip=128.135.24.119 --type="StunnelSubprocess"
                #s2cs --server_cert="/home/seena/scistream/server.crt" --server_key="/home/seena/scistream/server.key" --verbose  --listener-ip={args.p2cs_listener} --type={args.type} 2>&1 &
                #s2cs --verbose --port={args.sync_port} --listener-ip={args.p2cs_listener} --type={args.type} | tee /tmp/p2cs.log | tail -f /tmp/p2cs.log &
                #s2cs --verbose --port={args.sync_port} --listener-ip={args.p2cs_listener} --type={args.type} > /tmp/p2cs.log | tail -f /tmp.p2cs.log &
                #s2cs --verbose --port=5000 --listener-ip=128.135.24.119 --type="StunnelSubprocess" > /tmp/p2cs.log &
                #cat "$SCISTREAM_RSS_MAP"

                #   if [[ -n "$HAPROXY_CONFIG_PATH" ]]; then
                #    CONFIG_PATH="$HAPROXY_CONFIG_PATH"
                #else 
                #    CONFIG_PATH="/tmp/.scistream"
                #fi
                #if [[ -s "$CONFIG_PATH/resource.map" ]]; then
                #    rm -f "$CONFIG_PATH/resource.map"
                #fi

                #setsid stdbuf -oL -eL s2cs --server_cert="/home/seena/scistream/server.crt" --server_key="/home/seena/scistream/server.key" --verbose  --listener-ip={args.p2cs_listener} --type={args.type} > $CONFIG_PATH/p2cs.log &
                
                #while [[ ! -f "$CONFIG_PATH/resource.map" ]] || ! grep -q "Prod Listeners:" "$CONFIG_PATH/resource.map"; do
                #    sleep 1
                #done
                #cat "$CONFIG_PATH/resource.map



                #CONFIG_PATH="${{HAPROXY_CONFIG_PATH:-/tmp/.scistream}}" && echo "HAPROXY_CONFIG_PATH is: $HAPROXY_CONFIG_PATH"
                #while [[ ! -f "$CONFIG_PATH/resource.map" ]] || ! grep -q "Prod Listeners:" "$CONFIG_PATH/resource.map"; do
                #    sleep 1
                #done

    shell_function = ShellFunction(command, walltime=60)
    print(f"\nP2CS:      Starting the Producer's S2CS on endpoint ({endpoint_name}) with the following args:\n {args}\n")
    with Executor(endpoint_id=uuid) as gce:
        future = gce.submit(shell_function)

        try:
            stream_uid, p2cs_sync, lstn_val = None, None, None

            while not future.done(): 
                result = future.result() 
                lines = result.stdout.strip().split("\n")

                for line in lines:
                    """if "Sync Port:" in line and p2cs_sync is None:
                        try:
                            p2cs_sync = line.split()[2]
                            result_q.put(("sync", p2cs_sync))
                            print(f"Found Sync: {p2cs_sync}")
                        except (IndexError, ValueError):
                            print("can't extract Sync Port from the Resource Map:", line)"""

                    if "Request UID" in line  and stream_uid is None:
                        try:
                            stream_uid = line.split()[2]
                            result_q.put(("uuid", stream_uid))
                            print(f"P2CS:      The captured scistream uid from the resource map is: {stream_uid}")
                        except IndexError:
                            print("can't extract UUID:", line)

                    elif "Listeners:" in line and lstn_val is None:
                        try:
                            # Extract everything inside brackets using regex
                            match = re.search(r"\[([^\]]+)\]", line)
                            if match:
                                raw_list = match.group(1)  # Extract content inside brackets
                                # Extract only port numbers and remove extra quotes
                                lstn_val = [entry.split(":")[-1].strip("'").strip('"') for entry in raw_list.split(", ")]
                                result_q.put(("ports", lstn_val))
                                print(f"P2CS:      The captured scistream listen ports from the resource map: {', '.join(lstn_val)}") 
                            else:
                                print("P2CS:      Listeners format is incorrect:", line)

                        except (SyntaxError, ValueError) as e:
                            print(f"Can't parse the ports: {e} | in the line: {line}")


                #if stream_uid and lstn_val and p2cs_sync:
                if stream_uid and lstn_val:
                    break

                time.sleep(1)  
            #print(result.stdout, flush=True)

        except Exception as e:
            print(f"P2CS:      Task failed: {e}")
            #return None  #None if no key is found

        #finally:
        #    print("Cleaning up Executor resources.")





def c2cs(args, endpoint_name, uuid, scistream_uuid, port_list, results_queue):

    from globus_compute_sdk import Executor, ShellFunction, Client
    from globus_compute_sdk.sdk.executor import ComputeFuture
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    import signal
    from datetime import datetime
    import sys, socket, time, os

    command =   f"""
                bash -c '
                if [[ -n "$HAPROXY_CONFIG_PATH" ]]; then
                    CONFIG_PATH="$HAPROXY_CONFIG_PATH"
                else 
                    CONFIG_PATH="/tmp/.scistream"
                fi
                mkdir -p "$CONFIG_PATH"
                stdbuf -oL -eL s2cs --server_crt="$CONFIG_PATH/server.crt" --server_key="$CONFIG_PATH/server.key" --verbose  --listener_ip={args.c2cs_listener} --type={args.type} > "$CONFIG_PATH/c2cs.log" &
                echo "stunnel started in the c2cs with PID: $(pgrep -x stunnel)" 
                '
                """
                #the correct command: s2cs --server_crt="$CONFIG_PATH/server.crt" --server_key="$CONFIG_PATH/server.key" --verbose --listener_ip=128.135.24.120 --type="StunnelSubprocess"


    shell_function = ShellFunction(command, walltime=60)
    print(f"\nC2CS:      Starting the Consumer's S2CS on endpoint ({endpoint_name}) with the following args:\n {args}\n")

    with Executor(endpoint_id=uuid) as gce:
        future = gce.submit(shell_function)

        try:
            result = future.result()
            print(f"Stdout: \n{result.stdout}", flush=True)
            cln_stderr = "\n".join(line for line in result.stderr.split("\n") if "WARNING" not in line)
            if cln_stderr.strip():
                print(f"Stderr: {cln_stderr}", flush=True)
        except Exception as e:
            print(f"C2CS:      Task failed on: {e}")
        #finally:
        #    print("Cleaning up Executor resources.")





def conin(args, endpoint_name, uuid, result_q):
    
    import time
    from globus_compute_sdk import Executor, Client, ShellFunction
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import datetime
    import sys, socket, queue

    command =   f"""
                bash -c '
                if [[ -n "$HAPROXY_CONFIG_PATH" ]]; then
                    CONFIG_PATH="$HAPROXY_CONFIG_PATH"
                else 
                    CONFIG_PATH="/tmp/.scistream"
                fi
                mkdir -p "$CONFIG_PATH"
                sleep 5
                s2uc inbound-request --server_cert="$CONFIG_PATH/server.crt" --remote_ip {args.prod_ip} --s2cs {args.p2cs_ip}:5000  > "$CONFIG_PATH/conin.log" &
                '
                """
                
                #correct command: s2uc inbound-request --server_cert="$CONFIG_PATH/server.crt" --remote_ip 128.135.24.117 --s2cs 128.135.164.119:5000
                #  --s2cs 128.135.164.119:5000 #if you add receiver_ports it will only activate it on the mentioned port! --receiver_ports 5074
                #s2uc inbound-request --remote_ip 128.135.24.117 --s2cs 128.135.164.119:5000 &
                #s2uc inbound-request --remote_ip {args.prod_ip} --s2cs {args.p2cs_ip}:5000 > /tmp/conin.log & '

    shell_function = ShellFunction(command, walltime=60)
    print(f"\nCON IN:      Starting the Consumer's inbound connection on endpoint ({endpoint_name}) with the following args:\n {args}\n")

    with Executor(endpoint_id=uuid) as gce:
        future = gce.submit(shell_function)

        try:
            result = future.result(timeout=60)
            #print(f"Stdout: \n{result.stdout}")
            #cln_stderr = "\n".join(line for line in result.stderr.split("\n") if "WARNING" not in line)
            #if cln_stderr.strip():
            #    print(f"Stderr: {cln_stderr}", flush=True)

        except Exception as e:
            print(f"CON IN:      Task failed: {e}")
        """finally:
            print("CON IN:      Cleaning up Executor resources.")"""



def conout(args, endpoint_name, uuid, scistream_uuid, port_list, results_queue):
    
    import time
    from globus_compute_sdk import Executor, Client, ShellFunction
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import datetime
    import sys, socket, queue

    if scistream_uuid is None or port_list is None:
        print(f"CON OUT:     Error: Required values missing. Exiting: {stream_uid} and {outbound_ports}")
        exit(1)

    print(f"\nCON OUT:      Received port_list {port_list} and scistream_uuid {scistream_uuid} in conout!")
    first_port = port_list[0]

    command =   f"""
                bash -c '
                if [[ -n "$HAPROXY_CONFIG_PATH" ]]; then
                    CONFIG_PATH="$HAPROXY_CONFIG_PATH"
                else 
                    CONFIG_PATH="/tmp/.scistream"
                fi
                mkdir -p "$CONFIG_PATH"
                echo "in bash of conout s2uc outbound will start with the uid {scistream_uuid} and on the port {first_port}"
                sleep 5
                s2uc outbound-request --server_cert="$CONFIG_PATH/server.crt" --remote_ip {args.p2cs_ip} --s2cs {args.c2cs_listener}:5000  --receiver_ports={first_port} {scistream_uuid} {args.p2cs_ip}:{first_port}  > "$CONFIG_PATH/conout.log" &
                '
                """
                #correct command: s2uc outbound-request --server_cert="/home/seena/scistream/server.crt" --remote_ip 128.135.164.119 --s2cs 128.135.24.120:5000  --receiver_ports=5100 0cddc36c-f3b5-11ef-9275-aee3018ac00c 128.135.164.119:5100
                #s2uc outbound-request --server_cert="$CONFIG_PATH/server.crt" --remote_ip 128.135.164.119 --s2cs 128.135.24.120:5000 d1d55174-eefd-11ef-ae06-aee3018ac00c --receiver_ports=5100  128.135.164.119:5100  &
                #s2uc outbound-request --server_cert="$CONFIG_PATH/server.crt" --remote_ip {args.p2cs_ip} --s2cs {args.c2cs_listener}:{args.sync_port} --receiver_ports {port_list[0]} {scistream_uuid}  {args.p2cs_ip}:{port_list[0]}  > /tmp/c2uc.log 2>&1 '

    shell_function = ShellFunction(command, walltime=60)
    print(f"\nCON OUT:      Starting the Consumer's outbound connection on endpoint ({endpoint_name}) with the following args:\n {args}\n")
    print(f"\nCON OUT:      Starting the Consumer's outbound connection on endpoint ({scistream_uuid}) with the following args:\n {port_list}\n")


    with Executor(endpoint_id=uuid) as gce:
        future = gce.submit(shell_function)

        try:
            result = future.result(timeout=60)
            #print(f"Stdout: \n{result.stdout}")
            #cln_stderr = "\n".join(line for line in result.stderr.split("\n") if "WARNING" not in line)
            #if cln_stderr.strip():
            #    print(f"Stderr: {cln_stderr}", flush=True)

        except Exception as e:
            print(f"CON OUT:      Task failed: {e}")
"""        finally:
            print("Cleaning up Executor resources.")"""












"""
def con(args, uuid):
    
    import time
    from globus_compute_sdk import Executor, Client, ShellFunction
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import datetime
    import sys, socket

    command =   f
                timeout 60 bash -c '
                sleep 5
                s2uc inbound-request --remote_ip 128.135.24.117 --s2cs 128.135.164.119:5000 & >> /tmp/c2us.log 2>&1 &
                sleep 5
                s2uc outbound-request --remote_ip 128.135.164.119 --s2cs 128.135.24.120:5000 d1d55174-eefd-11ef-ae06-aee3018ac00c --receiver_ports=5100  128.135.164.119:5100  & > /tmp/appctrl.log 2>&1 '
                

    shell_function = ShellFunction(command, walltime=60)

    with Executor(endpoint_id=uuid) as gce:
        future = gce.submit(shell_function)

        try:
            result = future.result(timeout=60)
            #print("CONS ---------------------------------")
            print(f"Stdout: \n{result.stdout}")
            cln_stderr = "\n".join(line for line in result.stderr.split("\n") if "WARNING" not in line)
            if cln_stderr.strip():
                print(f"Stderr: {cln_stderr}", flush=True)
            #print("CONS ---------------------------------")
        except Exception as e:
            print(f"Task failed: {e}")
"""


"""
from globus_compute_sdk import Client
gcc = Client()
task_id = gcc.get_worker_hardware_details(ep_uuid)
# wait some time...
print(gcc.get_result(task_id))

from globus_compute_sdk import Client

def expensive_task(task_arg):
    import time
    time.sleep(3600 * 24)  # 24 hours
    return "All done!"

ep_id = "<endpoint_id>"
gcc = Client()

print(f"Task Group ID for later reloading: {gcc.session_task_group_id}")
fn_id = gcc.register_function(expensive_task)
batch = gcc.create_batch()
for task_i in range(10):
    batch.add(fn_id, ep_id, args=(task_i,))
gcc.batch_run(batch)
"""












"""
                bash -c '
                stdbuf -oL -eL s2cs --verbose --port={args.sync_port} --listener-ip={args.p2cs_listener} --type={args.type} > /tmp/p2cs.log &      
                while ! grep -q "req started, with request uid:" /tmp/p2cs.log; do
                    sleep 1
                done
                echo "and conf path is $HAPROXY_CONFIG_PATH "
                sync_port=$(grep "Secure Server started on" /tmp/p2cs.log | head -n 1 | cut -d " " -f5)
                uid_key=$(grep "req started, with request uid:" /tmp/p2cs.log | head -n 1 | cut -d " " -f6)
                echo "Extracted UUID: $uid_key"
                echo "Extracted SYNC: $sync_port"
                while ! grep -q "Available ports:" /tmp/p2cs.log; do
                    sleep 1
                done
                listn_ports=$(grep "Available ports:" /tmp/p2cs.log | head -n 1 | cut -d " " -f3,4,5,6,7)
                echo "Extracted PORTS:  $listn_ports"
                '
                """


""" 
    timeout 60 bash -c '
    nohup s2cs --verbose --port={args.sync_port} --listener-ip={args.p2cs_listener} --type={args.type} > /tmp/p2cs.log 2>&1 &
    s2cs --verbose --port={args.sync_port} --listener-ip={args.p2cs_listener} --type={args.type} | tee /tmp/p2cs.log 2>&1
    cat /tmp/p2cs.log
    PID0=$(pgrep -f "nohup")
    stdbuf -oL echo "pid0 is nohup: " $PID0
    PID1=$!
    stdbuf -oL echo "pid1 is nohup: " $PID1
    PID2=$(pgrep -f "s2cs --verbose --port={args.sync_port}")
    stdbuf -oL echo "pid2 is s2cs: " $PID2
    PPID1=$(ps -o ppid= -p $PID | tail -n 1 | tr -d ' ')
    PPID2=$(ps -o ppid= -p $PID | awk 'NR==2 {print $1}')
    echo "p2cs pid and ppid1 and ppid2 are" $PID2 $PID1 $PPID1 $PPID2
    echo $PID1 $PID2 $PPID1 $PPID2 >> /tmp/p2cs.pids
    echo "S2CS PID in P2CS is " $!
    cat /tmp/p2cs.log
    sleep 50
    kill -9 $(cat /tmp/p2cs.pid)
    #rm -f /tmp/p2cs.pid
    cat /tmp/p2cs.log '

                echo "S2CS PID in P2CS is " $!
                echo "S2CS PID in pid file is " $! > /tmp/p2cs.pid
                cat /tmp/p2cs.pid

                        PPID1=$(ps -ep ppid= -p $PID1)
                    PPID2=$(ps -ep ppid= -p $PID2)




                while ! pgrep -f "stunnel" > /dev/null; do
                    echo "waiting"
                    sleep 1
                done

                STUNNEL_PID=$(pgrep -f "stunnel")
                STUNNEL_PPID=$(ps -o ppid= -p $STUNNEL_PID | tr -d " ")

                stdbuf -oL echo "stunnel pid in p2cs is : " $STUNNEL_PID
                stdbuf -oL echo "stunnel ppid in p2cs is : " $STUNNEL_PPID




                s2cs --verbose --port={args.sync_port} --listener-ip={args.p2cs_listener} --type={args.type} > /tmp/p2cs.log &
                stdbuf -oL echo "s2cs pid in p2cs is : " $! && echo $! > /tmp/p2cs.pid
                stdbuf -oL echo "s2cs ppid in p2cs is : " $(ps -o ppid= -p $!) && echo $(ps -o ppid= -p $!) > /tmp/p2cs.ppid

                while ! grep -q "req started, with request uid:" /tmp/p2cs.log; do
                    sleep 1
                done
                key=$(grep "req started, with request uid:" /tmp/p2cs.log | head -n 1 | cut -d " " -f6)
                stdbuf -oL echo "Extracted Key: $key"

                STUNNEL_PID=$(pgrep -f "stunnel") && stdbuf -oL echo "stunnel pid in p2cs is : which is not working properly" $STUNNEL_PID
                STUNNEL_PPID=$(ps -o ppid= -p $STUNNEL_PID | tr -d " ") && stdbuf -oL echo "stunnel ppid in p2cs is : which is not working properly" $STUNNEL_PPID
                cat /tmp/p2cs.log
    """










"""working version for resource.map


                timeout 60 bash -c '
                stdbuf -oL -eL s2cs --server_cert="/home/seena/scistream/server.crt" --server_key="/home/seena/scistream/server.key" --verbose  --listener-ip={args.p2cs_listener} --type={args.type} &

                if [ -n "$HAPROXY_CONFIG_PATH" ] && [ -s "$HAPROXY_CONFIG_PATH/resource.map" ]; then
                    CONFIG_PATH="$HAPROXY_CONFIG_PATH/resource.map"
                else
                    CONFIG_PATH="/tmp/.scistream/resource.map"
                fi
                while ! grep -q "Prod Listeners:" "$CONFIG_PATH"; do
                    sleep 1
                done
                cat "$CONFIG_PATH"
                sleep 5
                rm -f $CONFIG_PATH

if "Sync Port:" in line and p2cs_sync is None:
                        try:
                            p2cs_sync = line.split()[2]
                            result_q.put(("sync", p2cs_sync))
                            print(f"Found Sync: {p2cs_sync}")
                        except (IndexError, ValueError):
                            print("can't extract Sync Port from the Resource Map:", line) doubel check this one!!!


                        if "Request UID" in line  and stream_uid is None:
                        try:
                            stream_uid = line.split()[2]
                            result_q.put(("uuid", stream_uid))
                            print(f"Found Key: {stream_uid}")
                        except IndexError:
                            print("can't extract UUID:", line)

                    elif "Listeners:" in line and lstn_val is None:
                        try:
                            # Extract everything inside brackets using regex
                            match = re.search(r"\[([^\]]+)\]", line)
                            if match:
                                raw_list = match.group(1)  # Extract content inside brackets
                                # Extract only port numbers and remove extra quotes
                                lstn_val = [entry.split(":")[-1].strip("'").strip('"') for entry in raw_list.split(", ")]
                                result_q.put(("ports", lstn_val))
                                print(f"Found Ports: {', '.join(lstn_val)}") 
                            else:
                                print("Listeners format is incorrect:", line)

                        except (SyntaxError, ValueError) as e:
                            print(f"Can't parse the ports: {e} | in the line: {line}")"""



"""working version for logfile:


                stdbuf -oL -eL s2cs --server_cert="/home/seena/scistream/server.crt" --server_key="/home/seena/scistream/server.key" --verbose --listener-ip={args.p2cs_listener} --type={args.type} 



                    if "req started, with request uid:" in line and stream_uid is None:
                        match = re.search(r'uid:\s*"([a-f0-9-]+)"', line)
                        if match:
                            stream_uid = match.group(1)
                            result_q.put(("uuid", stream_uid))
                            print(f"Found Key ehich is the uid: {stream_uid}")
                        else:
                            print("Can't extract UUID:", line)


                    elif "Available ports:" in line and lstn_val is None:
                        # Extract everything inside brackets using regex
                        match = re.search(r"\[([^\]]+)\]", line)
                        if match:
                            raw_list = match.group(1)  # Extract content inside brackets
                            # Extract only port numbers and remove extra quotes
                            lstn_val = [entry.strip() for entry in raw_list.split(", ")]
                            result_q.put(("ports", lstn_val))
                            print(f"Found Ports: {', '.join(lstn_val)}") 
                        else:
                            print("Listeners format is incorrect:", line)

"""
