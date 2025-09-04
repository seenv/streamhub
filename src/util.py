from __future__ import annotations
import logging, os, shlex, time, uuid
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