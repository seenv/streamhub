def p2cs(args, uuid):

    from globus_compute_sdk import Executor, ShellFunction, Client
    from globus_compute_sdk.sdk.executor import ComputeFuture
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    import signal
    from datetime import datetime
    import sys, socket, time, os

    command =   f"""
                timeout 60 bash -c '
                nohup s2cs --verbose --listener-ip={args.p2cs_listener} --type={args.type} > /tmp/p2cs.log 2>&1 &
                echo $! > /tmp/p2cs.pid
                echo "S2CS PID in P2CS is " $!
                '
                """

    shell_function = ShellFunction(command, walltime=60)

    with Executor(endpoint_id=uuid) as gce:
        future = gce.submit(shell_function)

        try:
            result = future.result(timeout=60)
            print(f"Stdout: \n{result.stdout}", flush=True)
            cln_stderr = "\n".join(line for line in result.stderr.split("\n") if "WARNING" not in line)
            if cln_stderr.strip():
                print(f"Stderr: {cln_stderr}", flush=True)

            
        except Exception as e:
            print(f"Task failed: {e}")




def c2cs(args, uuid):

    from globus_compute_sdk import Executor, ShellFunction, Client
    from globus_compute_sdk.sdk.executor import ComputeFuture
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    import signal
    from datetime import datetime
    import sys, socket, time, os

    command =   f"""
                timeout 60 bash -c '
                nohup s2cs --verbose --listener-ip={args.p2cs_listener} --type={args.type} > /tmp/p2cs.log 2>&1 &
                echo $! > /tmp/p2cs.pid
                echo "S2CS PID in P2CS is " $! '
                """

    shell_function = ShellFunction(command, walltime=60)

    with Executor(endpoint_id=uuid) as gce:
        future = gce.submit(shell_function)

        try:
            result = future.result(timeout=60)
            print(f"Stdout: \n{result.stdout}", flush=True)
            cln_stderr = "\n".join(line for line in result.stderr.split("\n") if "WARNING" not in line)
            if cln_stderr.strip():
                print(f"Stderr: {cln_stderr}", flush=True)
        except Exception as e:
            print(f"Task failed: {e}")




def pub(args, uuid):

    import time
    from globus_compute_sdk import Executor, Client, ShellFunction
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import datetime
    import sys, socket

    command =   f"""
                timeout 60 bash -c '
                sleep 5
                s2uc prod-req --s2cs {args.p2cs_listener}:{args.sync_port} --mock True > /tmp/p2us.log 2>&1 &
                sleep 5
                appctrl mock 4f8583bc-a4d3-11ee-9fd6-034d1fcbd7c3 {args.p2cs_listener}:{args.sync_port} INVALID_TOKEN PROD {args.prod_ip}  > /tmp/appctrl.log 2>&1 '
                """
    
    shell_function = ShellFunction(command, walltime=60)

    with Executor(endpoint_id=uuid) as gce:
        future = gce.submit(shell_function)

        try:
            result = future.result(timeout=60)
            #print("PROD ---------------------------------")
            print(f"Stdout: \n{result.stdout}")
            cln_stderr = "\n".join(line for line in result.stderr.split("\n") if "WARNING" not in line)
            if cln_stderr.strip():
                print(f"Stderr: {cln_stderr}", flush=True)
            #print("PROD ---------------------------------")
        except Exception as e:
            print(f"Task failed: {e}")




def con(args, uuid):
    
    import time
    from globus_compute_sdk import Executor, Client, ShellFunction
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import datetime
    import sys, socket

    command =   f"""
                timeout 60 bash -c '
                sleep 15
                s2uc inbound-request --remote_ip 128.135.24.117 --s2cs 128.135.164.119:5000 & > /tmp/p2us.log 2>&1 &
                sleep 5
                s2uc outbound-request --remote_ip 128.135.164.119 --s2cs 128.135.24.120:5000 d1d55174-eefd-11ef-ae06-aee3018ac00c --receiver_ports=5100  128.135.164.119:5100  & > /tmp/c2uc.log 2>&1 '
                """

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