def pub(args, uuid):

    import time
    from globus_compute_sdk import Executor, Client, ShellFunction
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import datetime
    import sys, socket

    #gcc = Client()

    command = f"""
    timeout 60 bash -c '
    globus-compute-endpoint list &&
    sleep 5
    s2uc prod-req --s2cs {args.p2cs_listener}:{args.sync_port} --mock True &
    sleep 5
    appctrl mock 4f8583bc-a4d3-11ee-9fd6-034d1fcbd7c3 {args.p2cs_listener}:{args.sync_port} INVALID_TOKEN PROD {args.prod_ip}  '
    """

    #endpoint_id = "45f5641d-d402-444a-a04c-20e8637ac259"

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