from __future__ import annotations
import os
from util import run_remote_debug as run_remote  # echoes submitted commands


def p2cs(args, uuid: str, *, sess_dir: str) -> dict:
    cmd = f"""
            bash -c '
            CERT_DIR="{sess_dir}/certs" && LOG_DIR="{sess_dir}/logs" && PROC_DIR="{sess_dir}/procs" && mkdir -p "$LOG_DIR" "$PROC_DIR"
            timeout 60s setsid stdbuf -oL -eL s2cs --server_crt=$CERT_DIR/server.crt --server_key=$CERT_DIR/server.key --verbose --listener_ip={args.p2cs_listener} --type="{args.type}" > "$LOG_DIR/p2cs.log" 2>&1 & echo $! > "$PROC_DIR/p2cs.pid"
            sleep 1 && cat "$HAPROXY_CONFIG_PATH/p2cs.log"
            '
            """
    return run_remote(uuid, "LAUNCH:p2cs", cmd, wall=90, wait=90)


def c2cs(args, uuid: str, *, sess_dir: str) -> dict:
    cmd = f"""
            bash -c '
            CERT_DIR="{sess_dir}/certs" && LOG_DIR="{sess_dir}/logs" && PROC_DIR="{sess_dir}/procs" && mkdir -p "$LOG_DIR" "$PROC_DIR"
            timeout 60s setsid stdbuf -oL -eL s2cs --server_crt=$CERT_DIR/server.crt --server_key=$CERT_DIR/server.key --verbose --listener_ip={args.c2cs_listener} --type="{args.type}" > "$LOG_DIR/c2cs.log" 2>&1 & echo $! > "$PROC_DIR/c2cs.pid"
            sleep 1 && cat "$HAPROXY_CONFIG_PATH/p2cs.log" && echo STARTED
            '
            """
    return run_remote(uuid, "LAUNCH:c2cs", cmd, wall=90, wait=90)


def inbound(args, role_label: str, runner_uuid: str, *, sess_dir: str) -> dict:
    ports = getattr(args, "inbound_src_ports", [])
    recv_ports_str = ",".join(str(p) for p in ports) if isinstance(ports, (list, tuple)) else str(ports).strip().strip("[]").replace(" ", "")
    cmd = f"""
            bash -c '
            CERT_DIR="{sess_dir}/certs" && LOG_DIR="{sess_dir}/logs" && PROC_DIR="{sess_dir}/procs" && mkdir -p "$LOG_DIR" "$PROC_DIR"
            timeout 10s s2uc inbound-request --server_cert=$CERT_DIR/server.crt --remote_ip {args.prod_ip} --num_conn {args.num_conn} --receiver_ports={recv_ports_str}  --s2cs {args.p2cs_ip}:{args.sync_port} > "$LOG_DIR/inbound.log" 2>&1 & echo $! > "$PROC_DIR/inbound.pid"
            while ! grep -q "prod_listeners:" "$LOG_DIR/inbound.log"; do sleep 1; done
            sleep 1
            cat "$LOG_DIR/inbound.log"
            '
            """
    r = run_remote(runner_uuid, f"INBOUND:{role_label}", cmd, wall=60, wait=60)
    if not r.get("ok"):
        return r

    out = (r.get("stdout") or "")
    import re
    uid = None
    listen_ports = []

    # UID at line start
    #m = re.search(r'^([a-f0-9-]{{36}})\b', out, re.MULTILINE)
    #if m:
    #    uid = m.group(1)
    # listeners: "0.0.0.0:5100"
    #listen_ports = re.findall(r'listeners:\s*"[^"]+:(\d+)"', out)
    
    _LISTENERS = re.findall(r'(?im)^listeners:\s*"([^"]+)"', out)
    #_PROD_LISTENERS = re.findall(r'(?im)^prod_listeners:\s*"([^"]+)"', out)
    _UID = re.search(r'^([a-f0-9-]{36})\s+.*INVALID_TOKEN PROD', out, re.MULTILINE)
    uid = _UID.group(1) if _UID else None
    listen_ports = [port.split(":")[-1] for port in _LISTENERS]
    #prod_ports = [port.split(":")[-1] for port in _PROD_LISTENERS]

    if not uid or not listen_ports:
        return {"ok": False, "error": "Failed to extract stream UID or listen ports", "stdout": out}
    return {"ok": True, "uid": uid, "listen_ports": listen_ports, "stdout": out}


def outbound(args, role_label: str, runner_uuid: str, *, stream_uid: str, ports: list[str], sess_dir: str | None = None) -> dict:
    sd = sess_dir or "/tmp/.scistream"
    CERT_DIR = f"{sd}/certs"
    c2cs_ip = args.c2cs_ip
    sync_port = int(args.sync_port)
    num_conn = int(args.num_conn)

    dst_ports = getattr(args, "outbound_dst_ports", [])
    if isinstance(dst_ports, (list, tuple)):
        recv_ports_str = ",".join(str(p) for p in dst_ports)
    else:
        recv_ports_str = str(dst_ports).strip().strip("[]").replace(" ", "")

    backends = ",".join(f"{args.p2cs_ip}:{p}" for p in (ports or [5100 + i for i in range(args.num_conn)]))

    cmd = f"""
            bash -c '
            CERT_DIR="{sess_dir}/certs" && LOG_DIR="{sess_dir}/logs" && PROC_DIR="{sess_dir}/procs" && mkdir -p "$LOG_DIR" "$PROC_DIR"
            timeout 10s s2uc outbound-request --server_cert=$CERT_DIR/server.crt --remote_ip {args.c2cs_ip} --num_conn {args.num_conn} --s2cs {args.c2cs_ip}:{args.sync_port}  --receiver_ports={recv_ports_str} "{stream_uid}" {backends}  > "$LOG_DIR/outbound.log" 2>&1 & echo $! > "$PROC_DIR/outbound.pid"
            while ! grep -q "Hello message sent successfully" cat "$LOG_DIR/outbound.log"; do sleep 1 ; done
            sleep 1
            cat "$LOG_DIR/outbound.log"
            '
            """
    r = run_remote(runner_uuid, f"OUTBOUND:{role_label}", cmd, wall=60, wait=60)
    if not r.get("ok"):
        return r
    return {"ok": True, "stdout": r.get("stdout", "")}