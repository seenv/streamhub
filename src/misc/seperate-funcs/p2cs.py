"""import time
from globus_compute_sdk import Executor, ShellFunction

def p2cs():
    log_file = "p2cs_output.log"
    commands = "s2cs --verbose --port=5007 --listener-ip=128.135.24.119 --type=Haproxy"
    endpoint_id = "df1658eb-1c81-4bb1-bc46-3a74f30d1ce1"

    shell_function = ShellFunction(commands)

    with Executor(endpoint_id=endpoint_id) as gce:
        with open(log_file, "w") as f:
            print(f"Executing on endpoint {endpoint_id}...", file=f)
            future = gce.submit(shell_function)
            print(f"Task submitted to endpoint {endpoint_id} with Task ID: {future.task_id}", file=f)
            f.flush()  # Ensure immediate write to file

        try:
            with open(log_file, "a") as f:
                print("Waiting for task completion...\n", file=f)
                f.flush()
                for _ in range(10):  
                    f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Checking task status...\n")
                    f.flush()
                    time.sleep(1)

                result = future.result()
                f.write("Task completed successfully!\n")
                f.write(f"Stdout:\n{result.stdout}\n")
                f.write(f"Stderr:\n{result.stderr}\n")
                f.flush()
        except Exception as e:
            with open(log_file, "a") as f:
                f.write(f"Task failed: {e}\n")
                f.flush()

def tail_log(log_file):
    with open(log_file, "r") as f:
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.5) 
                continue
            print(line, end="") """




"""from globus_compute_sdk import Executor, Client, ShellFunction

gcc = Client()

def p2cs():
    commands = "s2cs --verbose --port=5007 --listener-ip=128.135.24.119 --type=Haproxy"
    endpoint_id = "df1658eb-1c81-4bb1-bc46-3a74f30d1ce1"

    shell_function = ShellFunction(commands, walltime=120)
    func_id = gcc.register_function(shell_function) 

    task_id = gcc.run(endpoint_id=endpoint_id, function_id=func_id) 

    print(f"Task submitted successfully with Task ID: {task_id}")

    try:
        result = gcc.get_result(task_id)
        print("Task completed successfully!")
        print(f"Stdout:\n{result.stdout}")
        print(f"Stderr:\n{result.stderr}")
    except Exception as e:
        print(f"Task failed: {e}")"""





import time
from globus_compute_sdk import Executor, ShellFunction, Client
import threading
import signal
from globus_compute_sdk.sdk.executor import ComputeFuture

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

"""def out(future):
    try:
        while not future.done(): 
            result = future.result(timeout=1) 
            if result and result.stdout:
                print(result.stdout, end="", flush=True)
            time.sleep(0.5)  
    except Exception as e:
        print(f"Error reading output: {e}")"""

def p2cs(args, uuid):


    from globus_compute_sdk import Executor, ShellFunction, Client
    from globus_compute_sdk.sdk.executor import ComputeFuture
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    import signal
    from datetime import datetime
    import sys, socket, time, os
    
    """
    commands = "timeout 30 s2cs --verbose --port=5007 --listener-ip=128.135.24.119 --type=Haproxy"
    endpoint_id = "df1658eb-1c81-4bb1-bc46-3a74f30d1ce1"
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
        #cleanup_task(future, gcc)"""

    #endpoint_id = "df1658eb-1c81-4bb1-bc46-3a74f30d1ce1"
    #no hup in the bash command keeps the process running even after the shell exits
    command=f"""
    timeout 60 bash -c '
    echo " Starting P2CS ---------------------------------"
    globus-compute-endpoint list 
    nohup s2cs --verbose --port={args.sync_port} --listener-ip={args.p2cs_listener} --type={args.type} > /tmp/s2cs.log 2>&1 &
    echo $! > /tmp/s2cs.pid
    echo "S2CS PID in P2CS is " $!
    sleep 50
    kill -9 $(cat /tmp/s2cs.pid)
    rm -f /tmp/s2cs.pid
    echo " Killing P2CS ---------------------------------"'
    """
    #s2cs --verbose --port={args.sync_port} --listener-ip={args.p2cs_listener} --type={args.type} & '

    shell_function = ShellFunction(command, walltime=60)

    with Executor(endpoint_id=uuid) as gce:
        #print(f"Executing on endpoint {uuid}...")
        #print(f">>>>>>>>>>>>>>futures with this Task Group ID: {gce.task_group_id}")
        future = gce.submit(shell_function)
        #print(f" futures with this Task Group ID: {gce.task_group_id}")
        #print(f">>>>>>>>>>>>>>task submitted to endpoint {endpoint_id} with task ID: {future.task_id}")

    try:
        #print("Waiting for task completion...\n")
        result = future.result(timeout=65)
        print("Task completed successfully!")
        print(f"Stdout: {result.stdout}", flush=True)
        print(f"Stderr: {result.stderr}", flush=True)
    except Exception as e:
        print(f"Task failed: {e}")