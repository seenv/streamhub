def p2cs():

    import time
    from globus_compute_sdk import Executor, Client, ShellFunction
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import datetime
    import sys, socket

    #gcc = Client()


    commands = "timeout 60 s2cs --verbose --port=5007 --listener-ip=128.135.24.119 --type=Haproxy"

    endpoint_id = "df1658eb-1c81-4bb1-bc46-3a74f30d1ce1"

    shell_function = ShellFunction(commands)

    with Executor(endpoint_id=endpoint_id) as gce:
        print(f"Executing on endpoint {endpoint_id}...")
        future = gce.submit(shell_function)
        print(f"Task submitted to endpoint {endpoint_id} with Task ID: {future.task_id}")

    try:
        print("Waiting for task completion...\n")
        result = future.result(timeout=120)
        print("Task completed successfully!")
        print(f"Stdout: {result.stdout}")
        print(f"Stderr: {result.stderr}")
    except Exception as e:
        print(f"Task failed: {e}")