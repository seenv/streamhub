from __future__ import annotations
import logging, shlex, time, uuid, base64
from typing import Dict, Optional
from globus_compute_sdk import Executor, ShellFunction

DEFAULT_BASE = "/tmp/.scistream"

def test_endpoint(uuid: str):
    """
    Submits 'echo "hello"' to the endpoint
    Useful as a fast, explicit connectivity check during debugging
    """
    r = run_remote(uuid, "HELLO", 'echo "hello"')
    print(f"Test result for {uuid}: {r}")
    return r

def make_session_id() -> str:
    """Generate a sortable unique session id: YYYYMMDD-HHMMSS-<8hex>"""
    ts = time.strftime("%Y%m%d-%H%M%S")
    short = uuid.uuid4().hex[:8]
    return f"{ts}-{short}"

def session_dir(base: Optional[str], session_id: str) -> str:
    """Join base directory (or default) with session_id, without double slashes"""
    b = base or DEFAULT_BASE
    return f"{b.rstrip('/')}/{session_id}"

def _export_env(env: Dict[str, str]) -> str:
    """
    Produce a block of 'export KEY=VALUE' lines with proper shell-quoting
    for inclusion at the top of the remote script
    """
    if not env:
        return ""
    parts = []
    for k, v in env.items():
        parts.append(f'export {k}={shlex.quote(v)}')
    return "\n".join(parts)

def run_remote_debug(uuid: str, label: str, script: str, **kwargs):
    """Wrapper around run_remote that prints the exact script being submitted"""
    print(f"\n[DEBUG] Submitting to endpoint {uuid} ({label}):\n{script}\n{'-'*60}")
    return run_remote(uuid, label, script, **kwargs)

def run_remote(uuid_str: str, label: str, script_body: str, *,
               env: Optional[Dict[str, str]] = None,
               wall: int = 180, wait: int = 180) -> dict:
    """
    Submit a shell script to a Globus Compute endpoint and wait for results
    - Wraps the payload in 'bash -lc' for a login shell environment
    - Enables 'set -euo pipefail' for safer shell behavior
    - Allows passing environment variables via 'env' (exported before script)
    Returns a dict with ok/label/stdout/stderr or ok=False on exception
    """
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

def key_gen(args, endpoint_name: str, uuid: str, *, sess_dir: str) -> dict:
    """
    Generate a self-signed cert/key pair remotely via openssl and return the cert PEM
    CN is chosen based on which side we're on (producer/consumer)
    """
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
    """
    Write the other gateway cert PEM into sess_dir/certs/peer.crt on the remote host
    No need but added a python heredoc to safely base64-decode the payload
    """
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
    """
    Write a PSK file into sess_dir/certs/psk.secrets
    If no secret is provided, returns ok=True with 'skipped'
    """
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