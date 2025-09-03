
import argparse
import time
from datetime import datetime
from globus_compute_sdk import Executor, Client, ShellFunction
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys, socket




def get_args():
    parser = argparse.ArgumentParser(description="Getting the arguments")
    parser.add_argument('--calls', nargs='+', required=True, help="p2cs --arg1 val1 --arg2 val2 prod --arg1 val3 --arg3 val4)")

    
    args = parser.parse_args()
    calls = args.calls
    
    i = 0
    while i < len(calls):
        role = calls[i]
        if role == "p2cs":
            p2cs_parser = argparse.ArgumentParser(description="Arguments for p2cs", add_help=False)
            p2cs_parser.add_argument('--arg1', required=True)
            p2cs_parser.add_argument('--arg2', required=True)
            
            # Extract args for p2cs
            p2cs_args = []
            i += 1
            while i < len(calls) and not calls[i].startswith("--") and not calls[i] in ["p2cs", "prod", "c2cs", "cons"]:
                p2cs_args.append(calls[i])
                i += 1
            parsed_args = p2cs_parser.parse_args(p2cs_args)
            p2cs(parsed_args.arg1, parsed_args.arg2)

        elif role == "prod":
            prod_parser = argparse.ArgumentParser(description="Arguments for prod", add_help=False)
            prod_parser.add_argument('--arg1', required=True)
            prod_parser.add_argument('--arg3', required=True)
            
            # Extract args for prod
            prod_args = []
            i += 1
            while i < len(calls) and not calls[i].startswith("--") and not calls[i] in ["p2cs", "prod", "c2cs", "cons"]:
                prod_args.append(calls[i])
                i += 1
            parsed_args = prod_parser.parse_args(prod_args)
            prod(parsed_args.arg1, parsed_args.arg3)

        elif role == "c2cs":
            c2cs_parser = argparse.ArgumentParser(description="Arguments for c2cs", add_help=False)
            c2cs_parser.add_argument('--arg1', required=True)
            c2cs_parser.add_argument('--arg3', required=True)
            
            # Extract args for c2cs
            c2cs_args = []
            i += 1
            while i < len(calls) and not calls[i].startswith("--") and not calls[i] in ["p2cs", "prod", "c2cs", "cons"]:
                c2cs_args.append(calls[i])
                i += 1
            parsed_args = c2cs_parser.parse_args(c2cs_args)
            c2cs(parsed_args.arg1, parsed_args.arg3)

        elif role == "cons":
            cons_parser = argparse.ArgumentParser(description="Arguments for cons", add_help=False)
            cons_parser.add_argument('--arg1', required=True)
            cons_parser.add_argument('--arg3', required=True)
            
            # Extract args for cons
            cons_args = []
            i += 1
            while i < len(calls) and not calls[i].startswith("--") and not calls[i] in ["p2cs", "prod", "c2cs", "cons"]:
                cons_args.append(calls[i])
                i += 1
            parsed_args = cons_parser.parse_args(cons_args)
            cons(parsed_args.arg1, parsed_args.arg3)

        else:
            print(f"Unknown function: {role}")
            i += 1


def get_uuid(client, name):
    try:
        endpoints = client.get_endpoints()
        for ep in endpoints:
            endpoint_name = ep.get('name', '').strip()
            if endpoint_name == name.strip().lower():
                #print(f"\nfound {name}\n")
                return ep.get('uuid')
    except Exception as e:
        print(f"error fetching {name}: {str(e)}")
    return None



if __name__ == "__main__":
    gcc = Client()
    c2cs_args = get_args(c2cs)
    p2cs_args = get_args(p2cs)
    prod_args = get_args(prod)
    cons_args = get_args(cons)

    #endpoints = {"pub": "swell-guy", "sub": "this-guy"}
    endpoints = {"pub": "this", "p2cs": "that", "c2cs": "neat", "con": "swell"}
    ep_ips = {"this": "128.135.24.117", "swell": "128.135.24.118", "that":"128.135.164.119", "neat": "128.135.164.120"}
    commands = {"p2cs": p2cs_args, "pub": prod_args, "c2cs": c2cs_args, "con": cons_args}

    endpoint_ids = {key: get_uuid(gcc, name) for key, name in endpoints.items()}
    shell_functions = {key: ShellFunction(cmd) for key, cmd in commands.items()}

    with ThreadPoolExecutor(max_workers=len(endpoints)) as executor:
        future_to_endpoint = {
            executor.submit(endpoint_idsp[names], endpoint_ids[key], shell_func, commands[key]): key
            for key, shell_func in shell_functions.items()}







import argparse
import time
from datetime import datetime
from globus_compute_sdk import Executor, Client, ShellFunction
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys, socket

def run_task(endpoint_id, shell_function):
    with Executor(endpoint_id=endpoint_id) as gce:
        print(f"Executing on endpoint {endpoint_id}...")
        future = gce.submit(shell_function)
        print(f"Task submitted to endpoint {endpoint_id} with Task ID: {future.task_id}")
        try:
            print("Waiting for task completion...\n")
            result = future.result()
            print("Task completed successfully!")
            print(f"Stdout: {result.stdout}")
            print(f"Stderr: {result.stderr}")
        except Exception as e:
            print(f"Task failed: {e}")

def get_args():
    parser = argparse.ArgumentParser(description="Getting the arguments")
    parser.add_argument('--calls', nargs='+', required=True, help="Specify function calls and their arguments (e.g., p2cs --arg1 val1 --arg2 val2 prod --arg1 val3 --arg3 val4)")
    return parser.parse_args()

def get_uuid(client, name):
    try:
        endpoints = client.get_endpoints()
        for ep in endpoints:
            endpoint_name = ep.get('name', '').strip()
            if endpoint_name == name.strip().lower():
                return ep.get('uuid')
    except Exception as e:
        print(f"Error fetching {name}: {str(e)}")
    return None

if __name__ == "__main__":
    # Initialize Globus Compute Client
    gcc = Client()

    # Define endpoints and IP mappings
    endpoints = {"pub": "this", "p2cs": "that", "c2cs": "neat", "cons": "swell"}
    ep_ips = {"this": "128.135.24.117", "swell": "128.135.24.118", "that": "128.135.164.119", "neat": "128.135.164.120"}

    # Map commands to shell functions (placeholder for actual commands)
    shell_commands = {
        "p2cs": "echo Running p2cs task",
        "prod": "echo Running prod task",
        "c2cs": "echo Running c2cs task",
        "cons": "echo Running cons task"
    }

    # Get UUIDs for endpoints
    endpoint_ids = {key: get_uuid(gcc, name) for key, name in endpoints.items()}
    shell_functions = {key: ShellFunction(cmd) for key, cmd in shell_commands.items()}

    # Parse arguments
    args = get_args()
    calls = args.calls

    # Process function calls
    tasks = []
    i = 0
    while i < len(calls):
        role = calls[i]
        if role in shell_commands:
            # Extract arguments for the role
            task_args = []
            i += 1
            while i < len(calls) and not calls[i].startswith("--") and calls[i] not in shell_commands:
                task_args.append(calls[i])
                i += 1

            # Add task to the list
            tasks.append((role, endpoint_ids[role], shell_functions[role]))
        else:
            print(f"Unknown role: {role}")
            i += 1

    # Execute tasks in parallel
    with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
        futures = {executor.submit(run_task, endpoint_id, shell_func): role for role, endpoint_id, shell_func in tasks}
        for future in as_completed(futures):
            role = futures[future]
            try:
                future.result()
                print(f"Task for {role} completed successfully.")
            except Exception as e:
                print(f"Task for {role} failed: {e}")
