from __future__ import annotations
import logging, shlex, time, uuid, base64
from typing import Dict, Optional
from globus_compute_sdk import Executor, ShellFunction

DEFAULT_BASE = "/tmp/.scistream"

def test_endpoint(uuid: str):
    r = run_remote(uuid, "HELLO", 'echo "hello world"')
    print(f"Test result for {uuid}: {r}")
    return r

def make_session_id() -> str:
    ts = time.strftime("%Y%m%d-%H%M%S")
    short = uuid.uuid4().hex[:8]
    return f"{ts}-{short}"

def session_dir(base: Optional[str], session_id: str) -> str:
    b = base or DEFAULT_BASE
    return f"{b.rstrip('/')}/{session_id}"

def _export_env(env: Dict[str, str]) -> str:
    if not env:
        return ""
    parts = []
    for k, v in env.items():
        parts.append(f'export {k}={shlex.quote(v)}')
    return "\n".join(parts)

def run_remote_debug(uuid: str, label: str, script: str, **kwargs):
    print(f"\n[DEBUG] Submitting to endpoint {uuid} ({label}):\n{script}\n{'-'*60}")
    return run_remote(uuid, label, script, **kwargs)

def run_remote(uuid_str: str, label: str, script_body: str, *,
               env: Optional[Dict[str, str]] = None,
               wall: int = 180, wait: int = 180) -> dict:
    env_block = _export_env(env or {})
    cmd = f"""bash -lc '
          set -euo pipefail
          {env_block}
          {script_body}
          '"""
    with Executor(endpoint_id=uuid_str) as gce:
        fut = gce.submit(ShellFunction(cmd), walltime=wall)
        try:
            res = fut.result(timeout=wait)
            out = getattr(res, "stdout", "") or ""
            err = getattr(res, "stderr", "") or ""
            logging.debug("%s stdout: %s", label, out.strip())
            if err.strip():
                logging.debug("%s stderr: %s", label, err.strip())
            return {"ok": True, "label": label, "stdout": out, "stderr": err}
        except Exception as e:
            logging.exception("%s failed", label)
            return {"ok": False, "label": label, "error": str(e)}

def stop_since_marker(endpoint_name: str, uuid: str, *, pid_dir: str, marker: str, timeout_s: int = 5) -> dict:
    script = f"""
        set -e
        m="{pid_dir}/{marker}"
        [ -f "$m" ] || touch "$m"
        if compgen -G "{pid_dir}/*.pid" > /dev/null; then
          for f in {pid_dir}/*.pid; do
            [ -f "$f" ] || continue
            if [ "$f" -nt "$m" ]; then
              pid="$(cat "$f" || true)"
              if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then kill "$pid" || true; fi
            fi
          done
          end=$(( $(date +%s) + {timeout_s} ))
          while [ $(date +%s) -lt $end ]; do
            alive=0
            for f in {pid_dir}/*.pid; do
              [ -f "$f" ] || continue
              if [ "$f" -nt "$m" ]; then
                pid="$(cat "$f" || true)"
                if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then alive=1; fi
              fi
            done
            [ $alive -eq 0 ] && break
            sleep 1
          done
          for f in {pid_dir}/*.pid; do
            [ -f "$f" ] || continue
            if [ "$f" -nt "$m" ]; then
              pid="$(cat "$f" || true)"
              if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then kill -9 "$pid" || true; fi
            fi
          done
        fi
        echo "OK"
        """
    return run_remote(uuid, f"KILL:{endpoint_name}", script)

def key_gen(args, endpoint_name: str, uuid: str, *, sess_dir: str) -> dict:
    cn = args.p2cs_listener if endpoint_name.lower() == "thats" else args.c2cs_listener
    script = f"""
                mkdir -p "{sess_dir}/certs"
                openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
                -keyout "{sess_dir}/certs/server.key" \
                -out "{sess_dir}/certs/server.crt" \
                -subj "/CN={cn}" \
                -addext "subjectAltName=IP:{args.p2cs_ip}, IP:{args.prod_ip}, IP:{args.c2cs_listener}, IP:{args.c2cs_ip}, IP:{args.cons_ip}, IP:{args.inbound_ip}, IP:{args.outbound_ip}" 2>/dev/null
                cat "{sess_dir}/certs/server.crt"
                """
    r = run_remote(uuid, f"KEYGEN:{endpoint_name}", script)
    if not r.get("ok"): 
        return r
    pem = (r.get("stdout") or "").strip()
    if not pem.startswith("-----BEGIN CERTIFICATE-----"):
        return {"ok": False, "label": r.get("label"), "error": "No certificate in stdout"}
    return {"ok": True, "label": r.get("label"), "cert_pem": pem}

def crt_dist(args, endpoint_name: str, uuid: str, *, sess_dir: str, peer_cert_pem: str) -> dict:
    b64 = base64.b64encode(peer_cert_pem.encode("utf-8")).decode("ascii")
    script = f"""
mkdir -p "{sess_dir}/certs"
python3 - <<'PY'
import base64,os
p="{sess_dir}/certs/peer.crt"
os.makedirs(os.path.dirname(p), exist_ok=True)
with open(p,"wb") as f: f.write(base64.b64decode("{b64}".encode("ascii")))
print("OK")
PY
"""
    return run_remote(uuid, f"CRT-DIST:{endpoint_name}", script)

def key_dist(args, endpoint_name: str, uuid: str, *, sess_dir: str, psk_secret: str | None=None) -> dict:
    if not psk_secret:
        return {"ok": True, "label": f"KEY-DIST:{endpoint_name}", "skipped": True}
    b64 = base64.b64encode(psk_secret.encode("utf-8")).decode("ascii")
    script = f"""
mkdir -p "{sess_dir}/certs"
python3 - <<'PY'
import base64,os
p="{sess_dir}/certs/psk.secrets"
os.makedirs(os.path.dirname(p), exist_ok=True)
with open(p,"wb") as f: f.write(base64.b64decode("{b64}".encode("ascii")))
print("OK")
PY
"""
    return run_remote(uuid, f"KEY-DIST:{endpoint_name}", script)