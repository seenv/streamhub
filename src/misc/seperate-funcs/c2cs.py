import time
from globus_compute_sdk import Executor, ShellFunction, Client
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import signal
from datetime import datetime
import sys, socket
from globus_compute_sdk.sdk.executor import ComputeFuture


#def cleanup_task(task_id):
def cleanup_task(task_id, gcc):
    print(f"Canceling Task {task_id}...")
    gcc.cancel_task(task_id)
    
"""def output(task_id, gcc):
    printed_lines = set() 
    try:
        while not future.done(): 
            result = future.result(timeout=1)
            if result and result.stdout:
                for line in result.stdout.split("\n"):
                    if line not in printed_lines:
                        print(line)  
                        printed_lines.add(line)  
            time.sleep(0.5)
    except Exception as e:
        print(f"Error reading output: {e}")"""

def out(future):
    try:
        while not future.done(): 
            result = future.result(timeout=1) 
            if result and result.stdout:
                print(result.stdout, end="", flush=True)
            time.sleep(0.5)  
    except Exception as e:
        print(f"Error reading output: {e}")

def c2cs(args, uuid):



    from globus_compute_sdk import Executor, ShellFunction, Client
    from globus_compute_sdk.sdk.executor import ComputeFuture
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    import signal
    from datetime import datetime
    import sys, socket, time, os
    



    """commands = "timeout 30 s2cs --verbose --port=5007 --listener-ip=128.135.24.120 --type=Haproxy"
    endpoint_id = "c9485ce4-6af4-4fda-90cb-64aae4891432"

    #shell_function = ShellFunction(commands, stdout="output.log", stderr="error.log", walltime=120, snippet_lines=5000)
    shell_function = ShellFunction(commands, stdout="output.log", stderr="error.log", walltime=120)

    #gcc = Client()

    with Executor(endpoint_id=endpoint_id) as gce:
        print(f"Executing on endpoint {endpoint_id}...")
        future = gce.submit(shell_function)

        task_id = future.task_id

        print(f"Task submitted to endpoint {endpoint_id} with Task ID: {task_id}")

        #signal.signal(signal.SIGTERM, lambda sig, frame: cleanup_task(future, gcc))
        #signal.signal(signal.SIGINT, lambda sig, frame: cleanup_task(future, gcc))

        output_thread = threading.Thread(target=out, args=(future,))
        output_thread.start()

        try:
            result = future.result(timeout=60) 
            output_thread.join() 
            print("Task completed successfully!")
            print(f"Stdout:\n{result.stdout}")
            print(f"Stderr:\n{result.stderr}")
        except Exception as e:
            print(f"Task failed: {e}")

        #finally:
        #    cleanup_task(future, gcc) """

    #log_file = '/home/seena/.globus_compute/neat/tasks_working_dir/s2cs_output.log"
    #command=f"""
    #timeout 60 bash -c '
    #touch {log_file} &&
    #globus-compute-endpoint list | tee -a {log_file} &&
    #s2cs --verbose --port={args.sync_port} --listener-ip={args.c2cs_listener} --type={args.type} & | tee -a {log_file}'
    #"""

    #endpoint_id = "c9485ce4-6af4-4fda-90cb-64aae4891432"

    command=f"""
    timeout 60 bash -c '
    echo " Starting C2CS ---------------------------------"
    globus-compute-endpoint list
    nohup s2cs --verbose --port={args.sync_port} --listener-ip={args.c2cs_listener} --type={args.type} > /tmp/s2cs.log 2>&1 &
    echo $! > /tmp/s2cs.pid
    echo "S2CS PID in C2CS is " $!
    sleep 50
    kill -9 $(cat /tmp/s2cs.pid)
    rm -f /tmp/s2cs.pid 
    echo " Killing C2CS ---------------------------------"'
    """

    shell_function = ShellFunction(command, walltime=60)

    with Executor(endpoint_id=uuid) as gce:
        #print(f"Executing on EndPoint {uuid}...")
        #print(f" futures with this Task Group ID: {gce.task_group_id}")
        future = gce.submit(shell_function)
        #print(f"Task submitted to endpoint {endpoint_id} with Task ID: {future.task_id}")


    try:
        #print("Waiting for task completion...\n")
        result = future.result(timeout=65)
        print("Task completed successfully!")
        print(f"Stdout: {result.stdout}", flush=True)
        print(f"Stderr: {result.stderr}", flush=True)
    except Exception as e:
        print(f"Task failed: {e}")