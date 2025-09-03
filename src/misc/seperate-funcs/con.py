def con(args, uuid):
    
    import time
    from globus_compute_sdk import Executor, Client, ShellFunction
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import datetime
    import sys, socket

    #gcc = Client()

    command = f"""
    timeout 60 bash -c '
    \n
    globus-compute-endpoint list
    sleep 15
    s2uc cons-req --s2cs {args.c2cs_listener}:{args.sync_port} 4f8583bc-a4d3-11ee-9fd6-034d1fcbd7c3 {args.p2cs_ip}:5074 &
    sleep 5
    appctrl mock 4f8583bc-a4d3-11ee-9fd6-034d1fcbd7c3 {args.c2cs_listener}:{args.sync_port} INVALID_TOKEN PROD {args.p2cs_ip}'
    """

    shell_function = ShellFunction(command, walltime=60)

    with Executor(endpoint_id=uuid) as gce:
        #print(f"Executing on endpoint {uuid}...")
        future = gce.submit(shell_function)
        #print(f"Task submitted to endpoint {endpoint_id} with Task ID: {future.task_id}")

    try:
        #print("Waiting for task completion...\n")
        result = future.result()
        print("Task completed successfully!")
        print(f"Stdout: {result.stdout}")
        print(f"Stderr: {result.stderr}")
    except Exception as e:
        print(f"Task failed: {e}")





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