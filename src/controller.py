from __future__ import annotations
import argparse, logging, os, re, base64
from typing import Dict

from globus_compute_sdk import Client

from util import make_session_id, session_dir, run_remote, test_endpoint, stop_since_marker
from util import key_gen, crt_dist, key_dist
import launcher as setup_mod


def _normalize(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").strip().lower())


class StreamController:
    """
    Orchestrates one session:
      - Resolve & probe endpoints:
          p2cs (producer gateway), c2cs (consumer gateway),
          inbound runner (s2uc inbound), outbound runner (s2uc outbound)
      - Create session marker in real SciStream PID dir (e.g., ~/.scistream)
      - key_gen on gateways, cross-trust certs, optional PSK
      - Launch s2cs on both gateways
      - Wait for gateway sync ports; stage proper certs on runners
      - Run s2uc inbound on inbound runner; parse stream UID/ports
      - Run s2uc outbound on outbound runner
      - Cleanup only processes newer than the per-session marker
    """

    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.client = Client()

        # Resolve and probe four roles
        self.endpoints = self._resolve_and_probe_endpoints()

        # Directories and session identifiers
        self.session_id = make_session_id()
        self.sess_dir = session_dir(getattr(args, "session_base", None), self.session_id)
        self.pid_dir = os.path.expanduser(getattr(args, "pid_dir", "/tmp/.scistream"))
        self.marker_name = f".session-{self.session_id}.mark"

        # Cert PEMs produced by key_gen() on gateways (filled in setup_crypto)
        self.p2cs_cert_pem: str | None = None
        self.c2cs_cert_pem: str | None = None

        logging.info("Session directory: %s", self.sess_dir)
        logging.info("PID directory: %s", self.pid_dir)

    # ----------------------- Endpoint Resolution & Probing -----------------------

    def _resolve_and_probe_endpoints(self) -> Dict[str, str]:
        roles = {
            "p2cs": (self.args.p2cs_ep, getattr(self.args, "p2cs_id", "")),
            "c2cs": (self.args.c2cs_ep, getattr(self.args, "c2cs_id", "")),
            "inbound": (self.args.inbound_ep, getattr(self.args, "inbound_id", "")),
            "outbound": (self.args.outbound_ep, getattr(self.args, "outbound_id", "")),
        }

        visible = list(self.client.get_endpoints())
        name_to_id = self._build_name_index(visible)

        resolved: Dict[str, str] = {}
        for role, (name, eid_arg) in roles.items():
            eid = self._resolve_single(role, name, eid_arg, name_to_id, visible)
            self._probe_or_raise(role, name, eid)
            if role in ("p2cs", "c2cs"):
                resolved[name.lower()] = eid
            else:
                resolved[role] = eid
        return resolved

    @staticmethod
    def _build_name_index(visible: list[dict]) -> dict[str, str]:
        idx: dict[str, str] = {}
        for e in visible:
            nm = (e.get("name") or "").strip()
            eid = e.get("id") or e.get("uuid")
            if nm and eid:
                idx[_normalize(nm)] = eid
        return idx

    def _resolve_single(
        self,
        role: str,
        wanted_name: str,
        wanted_id: str,
        name_index: dict[str, str],
        visible: list[dict],
    ) -> str:
        if wanted_id:
            logging.info("[%s] Using explicit endpoint ID for %s: %s", role, wanted_name, wanted_id)
            return wanted_id

        norm = _normalize(wanted_name)
        if norm in name_index:
            eid = name_index[norm]
            logging.info("[%s] Resolved by exact name match: %s -> %s", role, wanted_name, eid)
            return eid
        for k, eid in name_index.items():
            if k.startswith(norm):
                logging.info("[%s] Resolved by prefix match: %s -> %s", role, wanted_name, eid)
                return eid
        for k, eid in name_index.items():
            if norm in k:
                logging.info("[%s] Resolved by substring match: %s -> %s", role, wanted_name, eid)
                return eid

        seen = "\n".join(
            f"- {(e.get('name') or '').strip()}  [{e.get('id') or e.get('uuid')}]"
            for e in visible
            if e.get("name") and (e.get("id") or e.get("uuid"))
        )
        raise RuntimeError(
            f"Could not resolve endpoint for role '{role}' with name '{wanted_name}'.\n"
            f"Visible endpoints:\n{seen}\n"
            f"Tip: pass an explicit ID via --{role}-id <UUID>."
        )

    @staticmethod
    def _probe_or_raise(role: str, name_label: str, endpoint_id: str) -> None:
        r = run_remote(endpoint_id, f"PROBE:{role}", "echo OK", wall=20, wait=20)
        if not r.get("ok"):
            raise RuntimeError(
                f"Endpoint probe failed for role '{role}' ({name_label} -> {endpoint_id}): {r.get('error')}"
            )
        out = (r.get("stdout") or "").strip()
        if "OK" not in out:
            raise RuntimeError(
                f"Endpoint probe returned unexpected output for role '{role}' "
                f"({name_label} -> {endpoint_id}): {out!r}"
            )
        logging.info("[%s] Probe succeeded: %s (%s)", role, name_label, endpoint_id)

    def _eid(self, ep_name: str) -> str:
        return self.endpoints[ep_name.lower()]

    def _runner_eid(self, which: str) -> str:
        # which in {"inbound","outbound"}
        return self.endpoints[which]

    def sanity_check(self):
        for role, ep in [("p2cs", self.args.p2cs_ep), ("c2cs", self.args.c2cs_ep)]:
            uuid = self._eid(ep)
            r = run_remote(uuid, f"TEST:{role}", 'echo "hello"')
            out = (r.get("stdout") or "").strip()
            print(f"[{role}] endpoint {ep} ({uuid}) responded: {out}")

    # ----------------------- Port Availability Check -----------------------------
    
    def _check_remote_ports_free(self, endpoint_id: str, ports: list[int], label: str) -> dict:
        port_list = ",".join(str(int(p)) for p in ports)
        script = f"""
python3 - <<'PY'
import socket, sys
ip = "0.0.0.0"
ports = [{port_list}]
busy = []
for p in ports:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((ip, p))   # if this fails, the port is taken or not bindable
    except OSError:
        busy.append(p)
    finally:
        try: s.close()
        except: pass
if busy:
    print("BUSY:" + ",".join(map(str, busy)))
    sys.exit(1)
print("OK")
PY
"""
        return run_remote(endpoint_id, f"PORTS:check:{label}", script)

    def verify_requested_ports_available(self) -> None:
        for role, ep_name in (("p2cs", self.args.p2cs_ep), ("c2cs", self.args.c2cs_ep)):
            ports = getattr(self.args, "outbound_dst_ports", []) or []
            if ports:
                r = self._check_remote_ports_free(self._eid(ep_name), ports, role)
                out = (r.get("stdout") or "").strip()
                if (not r.get("ok")) or out.startswith("BUSY:"):
                    busy = out.split("BUSY:", 1)[1] if "BUSY:" in out else "unknown"
                    raise RuntimeError(f"Requested ports not free on {ep_name}: {busy}")

    # ------------------------------ Markers -------------------------------------

    def create_remote_markers(self) -> Dict[str, dict]:
        test_endpoint(self._eid(self.args.p2cs_ep))
        test_endpoint(self._eid(self.args.c2cs_ep)) 
        results: Dict[str, dict] = {}
        for ep_name, role in ((self.args.p2cs_ep, "producer"), (self.args.c2cs_ep, "consumer")):
            uuid = self._eid(ep_name)
            script = f"""
            mkdir -p "{self.pid_dir}"
            touch "{self.pid_dir}/{self.marker_name}"
            echo "OK"
            """
            r = run_remote(uuid, f"MARKER:{role}", script)
            results[role] = r
            if not r.get("ok"):
                logging.error("Failed to create marker on %s (%s): %s", role, ep_name, r)
        return results
    
    def _find_latest_marker_name(self, endpoint_id: str) -> str | None:
        # Return basename of newest .session-*.mark in pid_dir, or None
        script = f'ls -1t "{self.pid_dir}"/.session-*.mark 2>/dev/null | head -n1 || true'
        r = run_remote(endpoint_id, "MARKER:find_latest", script)
        if not r.get("ok"):
            return None
        path = (r.get("stdout") or "").strip()
        if not path:
            return None
        import os
        return os.path.basename(path)

    def preclean_previous_session(self) -> dict:
        """Kill anything started after the latest prior marker on both gateways"""
        results = {}
        for role, ep_name in (("p2cs", self.args.p2cs_ep), ("c2cs", self.args.c2cs_ep)):
            eid = self._eid(ep_name)
            latest = self._find_latest_marker_name(eid)
            if not latest:
                results[role] = {"ok": True, "skipped": True, "reason": "no prior marker"}
                continue
            r = stop_since_marker(role, eid, pid_dir=self.pid_dir, marker=latest)
            results[role] = r
        return results

    def deep_clean_previous_session(self) -> dict:
        results = {}
        for role, ep_name in (("p2cs", self.args.p2cs_ep), ("c2cs", self.args.c2cs_ep)):
            eid = self._eid(ep_name)
            script = f"""
                        PID_BASE=/tmp/.scistream; shopt -s nullglob 
                        for f in $PID_BASE/*.pid; do pid=$(cat $f || : ) && [[ "$pid"  =~ ^[0-9]+$ ]] && kill -9 "$pid" 2>/dev/null || : ; done
                        echo OK
                    """
            results[role] = run_remote(eid, f"PRECLEAN:{role}", script) 
        return results
        
    # ------------------------------ Crypto --------------------------------------

    def setup_crypto(self) -> Dict[str, dict]:
        ep1 = self.args.p2cs_ep  # producer gateway (thats)
        ep2 = self.args.c2cs_ep  # consumer gateway (neat)

        r1 = key_gen(self.args, "p2cs", self._eid(ep1), sess_dir=self.sess_dir)
        if not r1.get("ok"):
            logging.error("key_gen failed on %s: %s", ep1, r1)
            return {"p2cs": r1}
        self.p2cs_cert_pem = r1.get("cert_pem")

        r2 = key_gen(self.args, "c2cs", self._eid(ep2), sess_dir=self.sess_dir)
        if not r2.get("ok"):
            logging.error("key_gen failed on %s: %s", ep2, r2)
            return {"p2cs": r1, "c2cs": r2}
        self.c2cs_cert_pem = r2.get("cert_pem")

        t1 = crt_dist(self.args, "p2cs", self._eid(ep1), sess_dir=self.sess_dir, peer_cert_pem=self.c2cs_cert_pem or "")
        t2 = crt_dist(self.args, "c2cs", self._eid(ep2), sess_dir=self.sess_dir, peer_cert_pem=self.p2cs_cert_pem or "")
        if not t1.get("ok"):
            logging.error("crt_dist failed on %s: %s", ep1, t1)
        if not t2.get("ok"):
            logging.error("crt_dist failed on %s: %s", ep2, t2)
        return {"p2cs": t1, "c2cs": t2}

    def distribute_psk(self) -> Dict[str, dict]:
        secret = (self.args.psk_secret or "").strip()
        if not secret:
            return {"p2cs": {"ok": True, "skipped": True}, "c2cs": {"ok": True, "skipped": True}}
        p2 = key_dist(self.args, "p2cs", self._eid(self.args.p2cs_ep), sess_dir=self.sess_dir, psk_secret=secret)
        c2 = key_dist(self.args, "c2cs", self._eid(self.args.c2cs_ep), sess_dir=self.sess_dir, psk_secret=secret)
        return {"p2cs": p2, "c2cs": c2}

    # --------------------------- Server Launch (s2cs) ---------------------------

    def launch_p2cs(self) -> dict:
        r = setup_mod.p2cs(self.args, self._eid(self.args.p2cs_ep), sess_dir=self.sess_dir)
        if not r.get("ok"):
            logging.error("Launch p2cs failed: %s", r)
        return r

    def launch_c2cs(self) -> dict:
        r = setup_mod.c2cs(self.args, self._eid(self.args.c2cs_ep), sess_dir=self.sess_dir)
        if not r.get("ok"):
            logging.error("Launch c2cs failed: %s", r)
        return r

    # ----------------------------- Helpers for connect --------------------------

    def _stage_cert_pem_on(self, endpoint_id: str, pem: str, dest_path: str = "/tmp/.scistream/server.crt") -> dict:
        b64 = base64.b64encode((pem or "").encode("utf-8")).decode("ascii")
        script = f"""python3 - <<'PY'
import base64, os
data = base64.b64decode("{b64}".encode("ascii"))
p = "{dest_path}"
os.makedirs(os.path.dirname(p), exist_ok=True)
with open(p, "wb") as f:
    f.write(data)
print("OK")
PY
"""
        return run_remote(endpoint_id, "CERT:stage", script)
    
    def _wait_port(self, endpoint_id: str, host: str, port: int, timeout_s: int = 60) -> dict:
        # Pass host/port via environment to avoid quoting/repr issues on remote.
        script = f"""H={host} P={int(port)} T={int(timeout_s)} python3 - <<'PY'
import os, socket, sys, time
h = os.environ["H"]
p = int(os.environ["P"])
deadline = time.time() + int(os.environ.get("T","60"))
ok = False
while time.time() < deadline:
    try:
        s = socket.create_connection((h, p), 2.0)
        s.close()
        ok = True
        break
    except Exception:
        time.sleep(1)
print("READY" if ok else "NOTREADY")
sys.exit(0 if ok else 1)
PY
"""
        return run_remote(endpoint_id, "PORT:wait", script, wall=timeout_s + 10, wait=timeout_s + 10)

    # ------------------------------- Connect (s2uc) -----------------------------

    def connect(self) -> Dict[str, dict]:
        # Wait for producer gateway port
        wp = self._wait_port(self._eid(self.args.p2cs_ep), self.args.p2cs_ip, int(self.args.sync_port), timeout_s=60)
        if not wp.get("ok") or "READY" not in (wp.get("stdout") or ""):
            return {"inbound": {"ok": False, "error": f"s2cs not listening at {self.args.p2cs_ip}:{self.args.sync_port}", "wait": wp}}

        # Stage producer cert ON INBOUND RUNNER exactly where setup.inbound reads it
        if not self.p2cs_cert_pem:
            return {"inbound": {"ok": False, "error": "Missing producer cert PEM"}}
        inbound_runner = self._runner_eid("inbound")
        sr = self._stage_cert_pem_on(inbound_runner, self.p2cs_cert_pem,
                                     dest_path=f"{self.sess_dir}/certs/server.crt")
        if not sr.get("ok"):
            return {"inbound": sr}

        # Run inbound
        r_in = setup_mod.inbound(self.args, "producer", inbound_runner, sess_dir=self.sess_dir)
        if not r_in.get("ok"):
            logging.error("Inbound failed: %s", r_in)
            return {"inbound": r_in}

        uid = r_in.get("uid")
        listen_ports = r_in.get("listen_ports") or []

        # Wait for consumer gateway port
        wc = self._wait_port(self._eid(self.args.c2cs_ep), self.args.c2cs_ip, int(self.args.sync_port), timeout_s=60)
        if not wc.get("ok") or "READY" not in (wc.get("stdout") or ""):
            return {"inbound": r_in, "outbound": {"ok": False, "error": f"s2cs not listening at {self.args.c2cs_ip}:{self.args.sync_port}", "wait": wc}}

        # Stage consumer cert ON OUTBOUND RUNNER exactly where setup.outbound reads it
        if not self.c2cs_cert_pem:
            return {"inbound": r_in, "outbound": {"ok": False, "error": "Missing consumer cert PEM"}}
        outbound_runner = self._runner_eid("outbound")
        sr2 = self._stage_cert_pem_on(outbound_runner, self.c2cs_cert_pem,
                                      dest_path=f"{self.sess_dir}/certs/server.crt")
        if not sr2.get("ok"):
            return {"inbound": r_in, "outbound": sr2}

        # Run outbound
        r_out = setup_mod.outbound(self.args, "consumer", outbound_runner, stream_uid=uid, ports=listen_ports, sess_dir=self.sess_dir)
        if not r_out.get("ok"):
            logging.error("Outbound failed: %s", r_out)
        return {"inbound": r_in, "outbound": r_out}

    # -------------------------------- Cleanup -----------------------------------

    def cleanup(self) -> Dict[str, dict]:
        pass
        # Only kill what is newer than the marker on gateways (where s2cs writes pids)
        #p = stop_since_marker("p2cs", self._eid(self.args.p2cs_ep), pid_dir=self.pid_dir, marker=self.marker_name)
        #c = stop_since_marker("c2cs", self._eid(self.args.c2cs_ep), pid_dir=self.pid_dir, marker=self.marker_name)
        #return {"p2cs": p, "c2cs": c}
        

