from __future__ import annotations

from util import run_remote
# from util import run_remote_debug as run_remote  # to echo submitted commands

def p2cs(args, uuid: str, *, sess_dir: str) -> dict:
    """
    Launch producer-side s2cs on the gateway endpoint
    - Creates cert/log/proc dirs under the session path
    - Starts s2cs in background, captures PID to procs/p2cs.pid
    - Tails the log briefly so caller can see early output
    """
    cmd = f"""
            CERT_DIR="{sess_dir}/certs" && LOG_DIR="{sess_dir}/logs" && PROC_DIR="{sess_dir}/procs" && mkdir -p "$LOG_DIR" "$PROC_DIR"
            setsid stdbuf -oL -eL s2cs \
            --server_crt="$CERT_DIR/server.crt" \
            --server_key="$CERT_DIR/server.key" --verbose \
            --listener_ip={args.p2cs_listener} \
            --type="{args.type}" > "$LOG_DIR/p2cs.log" 2>&1 & echo $! > "$PROC_DIR/p2cs.pid"
            sleep 1 && cat "$LOG_DIR/p2cs.log"
            """
    return run_remote(uuid, "LAUNCH:p2cs", cmd, wall=90, wait=90)

def c2cs(args, uuid: str, *, sess_dir: str) -> dict:
    """
    Launch consumer-side s2cs on the gateway endpoint
    - Creates cert/log/proc dirs under the session path
    - Starts s2cs in background, captures PID to procs/c2cs.pid
    - Tails the log briefly so caller can see early output
    """
    cmd = f"""
            CERT_DIR="{sess_dir}/certs" && LOG_DIR="{sess_dir}/logs" && PROC_DIR="{sess_dir}/procs" && mkdir -p "$LOG_DIR" "$PROC_DIR"
            setsid stdbuf -oL -eL s2cs \
                --server_crt="$CERT_DIR/server.crt" \
                --server_key="$CERT_DIR/server.key" --verbose \
                --listener_ip={args.c2cs_listener} \
                --type="{args.type}" > "$LOG_DIR/c2cs.log" 2>&1 & echo $! > "$PROC_DIR/c2cs.pid"
            sleep 1 && cat "$LOG_DIR/c2cs.log" && echo STARTED
            """
    return run_remote(uuid, "LAUNCH:c2cs", cmd, wall=90, wait=90)

def inbound(args, role_label: str, runner_uuid: str, *, sess_dir: str) -> dict:
    """
    Run s2uc inbound-request on the runner
    - Constructs receiver_ports from args.inbound_src_ports
    - Starts inbound in background, writes PID
    - Waits until the log mentions 'prod_listeners:' then returns the log
    - Parses stream UID and listen ports from the log
    """
    ports = getattr(args, "inbound_src_ports", [])
    recv_ports_str = ",".join(str(p) for p in ports) if isinstance(ports, (list, tuple)) else str(ports).strip().strip("[]").replace(" ", "")
    cmd = f"""
            CERT_DIR="{sess_dir}/certs" && LOG_DIR="{sess_dir}/logs" && PROC_DIR="{sess_dir}/procs" && mkdir -p "$LOG_DIR" "$PROC_DIR"
            s2uc inbound-request \
                --server_cert="$CERT_DIR/server.crt" --remote_ip {args.prod_ip} \
                --num_conn {args.num_conn} --receiver_ports={recv_ports_str}  \
                --s2cs {args.p2cs_ip}:{args.sync_port} > "$LOG_DIR/inbound.log" 2>&1 & echo $! > "$PROC_DIR/inbound.pid" 
            while ! grep -q "prod_listeners:" "$LOG_DIR/inbound.log"; do sleep 1; done 
            sleep 1 && cat "$LOG_DIR/inbound.log" 
            """
    r = run_remote(runner_uuid, f"INBOUND:{role_label}", cmd, wall=60, wait=60)
    if not r.get("ok"):
        return r

    out = (r.get("stdout") or "")
    import re
    uid = None
    listen_ports = []

    # Extract stream UID and listener endpoints from the log lines
    _UID = re.search(r'^([a-f0-9-]{36})\s+.*INVALID_TOKEN PROD', out, re.MULTILINE)
    _LISTENERS = re.findall(r'(?im)^listeners:\s*"([^"]+)"', out)
    uid = _UID.group(1) if _UID else None
    listen_ports = [port.split(":")[-1] for port in _LISTENERS]

    if not uid or not listen_ports:
        return {"ok": False, "error": "Failed to extract stream UID or listen ports", "stdout": out}
    return {"ok": True, "uid": uid, "listen_ports": listen_ports, "stdout": out}

def outbound(args, role_label: str, runner_uuid: str, *, stream_uid: str, ports: list[str], sess_dir: str | None = None) -> dict:
    """
    Run s2uc outbound-request on the runner
    - Builds receiver ports from args.outbound_dst_ports
    - Builds backend list from inbound listen ports (p2cs_ip:port,...)
    - Starts outbound in background, writes PID
    - Waits for success marker in the log, then returns the log
    """
    # Build receiver ports arg from args
    dst_ports = getattr(args, "outbound_dst_ports", [])
    if isinstance(dst_ports, (list, tuple)):
        recv_ports_str = ",".join(str(p) for p in dst_ports)
    else:
        recv_ports_str = str(dst_ports).strip().strip("[]").replace(" ", "")

    # Backends are the inbound listen ports on the producer gateway address
    backends = ",".join(f"{args.p2cs_ip}:{p}" for p in (ports or [5100 + i for i in range(args.num_conn)]))

    cmd = f"""
            CERT_DIR="{sess_dir}/certs" && LOG_DIR="{sess_dir}/logs" && PROC_DIR="{sess_dir}/procs" && mkdir -p "$LOG_DIR" "$PROC_DIR"
            s2uc outbound-request \
                --server_cert=$CERT_DIR/server.crt --remote_ip {args.c2cs_ip} \
                --num_conn {args.num_conn} --s2cs {args.c2cs_ip}:{args.sync_port}  \
                --receiver_ports={recv_ports_str} "{stream_uid}" {backends}  > "$LOG_DIR/outbound.log" 2>&1 & echo $! > "$PROC_DIR/outbound.pid"
            while ! grep -q "Hello message sent successfully" cat "$LOG_DIR/outbound.log"; do sleep 1 ; done 
            sleep 1 && cat "$LOG_DIR/outbound.log"
            """
    r = run_remote(runner_uuid, f"OUTBOUND:{role_label}", cmd, wall=60, wait=60)
    if not r.get("ok"):
        return r
    return {"ok": True, "stdout": r.get("stdout", "")}