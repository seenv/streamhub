from __future__ import annotations
import base64
from util import run_remote

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