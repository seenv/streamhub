


from globus_compute_sdk import Executor, ShellFunction, Client

def kill(endpoint_name, uuid):
    from globus_compute_sdk import Executor, ShellFunction

    #TODO: chenge it so it releases the s2cs with s2cs release
    command = f"""
                bash -c '
                echo "Starting kill script..."
                for pid in $(pgrep -f stunnel | grep -v globus); do
                    ppid=$(ps -o ppid= -p "$pid" | tr -d " ")
                    kill -9 "$ppid" && kill -9 "$pid"
                done
                echo "Kill script finished."
                '
                """

    shell_func = ShellFunction(command, walltime=60)

    with Executor(endpoint_id=uuid) as gce:
        future = gce.submit(shell_func)

        try:
            result = future.result(timeout=60)
            print(f"Stdout of {endpoint_name.capitalize()}:\n{result.stdout}", flush=True)

            clean_stderr = "\n".join(line for line in result.stderr.split("\n") if "WARNING" not in line)
            if clean_stderr.strip():
                print(f"Stderr of {endpoint_name.capitalize()}: {clean_stderr}", flush=True)

        except Exception as e:
            print(f"Task on {endpoint_name.capitalize()} failed: {e}")



if __file__:
    gcc = Client()
    endpoint_name = "that"
    uuid = "df1658eb-1c81-4bb1-bc46-3a74f30d1ce1"

    kill(endpoint_name, uuid)



"""echo "Starting kill script..."
            S2CS_PID=$(pgrep -f s2cs) && STU_PID=$(pgrep -f stunnel | grep -v grep)
            if [[ -n "$STU_PID" ]]; then
                ehco "STU_PID is $STU_PPID" && kill -9 $STU_PPID && echo "Killed stunnel PPID"
                ehco "STU_PID is $STU_PID" && kill -9 $STU_PID && echo "Kill stunnel PID"
            else
                echo "No Stunnel process found."
            fi
            if [[ -n "$S2CS_PID" ]]; then
                echo "Killing S2CS process with PID $S2CS_PID"
                    kill -9 $S2CS_PID 2>/dev/null || kill -9 $S2CS_PID
            else
                echo "No S2CS process found."
            fi
            echo "Kill script finished.
            
            
            
            
            
                            for pid in $(pgrep -f stunnel | grep -v globus); do
                    ppid=$(ps -o ppid= -p "$pid" | tr -d " ")
                    kill -9 "$pid" "$ppid"
                done
                
                """