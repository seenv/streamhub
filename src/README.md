# Stream Controller (src)

The controller resolves endpoints, generates and distributes certs, launches the two gateway proxies (`s2cs` on producer/consumer), waits for sync ports, then runs the runner tools (`s2uc inbound-request` → parses UID/ports → `s2uc outbound-request`). It supports pre-cleaning leftovers from the previous session and fast port availability checks.

## Contents

```
src/
  config.py       # CLI flags & defaults (endpoints, IPs, ports, paths, flags)
  controller.py   # StreamController: resolve→probe→crypto→launch→connect→(cleanup)
  launcher.py     # Thin wrappers to run s2cs/s2uc remotely and parse outputs
  util.py         # Globus Compute exec helpers, session IDs, PID cleanup, crypto IO
  main.py         # Entry point: args → controller → preclean/launch/connect
```

## How it works (high-level)

```
[config.py args] → StreamController
   1) Resolve endpoints (name → id) and probe by running `echo OK`
   2) Create per-session marker in PID dir: .session-<timestamp>-<id>.mark
   3) key_gen on gateways → get PEMs → cross-distribute peer certs (optional PSK)
   4) Launch s2cs on producer & consumer; wait for TCP sync-port readiness
   5) Stage producer cert on inbound runner → s2uc inbound-request
         └─ parse stream UID + listen ports from logs
   6) Stage consumer cert on outbound runner → s2uc outbound-request (uses UID/ports)
   7) (optional) Cleanup using marker-based PID culling
```

ASCII flow:

```
producer gateway (s2cs)  ←sync→  consumer gateway (s2cs)
        ▲                               ▲
        │ inbound-request (s2uc)        │ outbound-request (s2uc)
        │  (runner: inbound)            │  (runner: outbound)
        └─────── parse UID/ports ───────┘
```

## Key features

- **Endpoint resolution**: exact→prefix→substring matching, or force via `--*-id`.
- **Probe-before-use**: fails fast if an endpoint cannot execute commands.
- **Self-signed TLS**: per-session cert/key; cross-trust via peer cert copy.
- **Port checks**: validates requested ports (bind on `0.0.0.0`) before launching.
- **Cleanup**: kills processes from *previous* session marker or all the previous sessions.

## CLI (selected)

See `config.py` for a full list;

- Endpoints:
  - `--p2cs-ep thats` / `--c2cs-ep neat` / `--inbound-ep swell` / `--outbound-ep swell`
  - Optional: `--p2cs-id UUID`, `--c2cs-id UUID`, `--inbound-id UUID`, `--outbound-id UUID`
- Network/Listeners:
  - `--p2cs_ip`, `--c2cs_ip`, `--prod_ip`, `--cons_ip`
  - `--p2cs-listener`, `--c2cs-listener`
  - `--sync-port 5000`, `--num-conn 11`, `--rate`, `--livetime`
  - `--inbound-src-ports 5074,...` (CSV), `--outbound-dst-ports 5100,...` (CSV)
  - `--type StunnelSubprocess`
- Paths:
  - `--session-base /tmp/.scistream` (per-session root)
  - `--pid-dir /tmp/.scistream` (where `.pid` and marker files live)
- Flags:
  - `--cleanup` (pre-clean previous session’s leftovers before starting)
  - `--no-deep-clean` (skip killing all the processes (and tunnels) from previus sessions)

## Quick start

1. Configure/confirm endpoints are visible to your Globus identity:
   ```bash
   # your endpoint names should match --p2cs-ep/--c2cs-ep/--inbound-ep/--outbound-ep
   ```

2. Run:
   ```bash
   cd src
   python3 main.py \
     --p2cs-ep p2cs \
     --c2cs-ep c2cs \
     --inbound-ep local \
     --outbound-ep local \
     --p2cs_ip 192.168.20.10 \
     --c2cs_ip 192.168.20.11 \
     --prod_ip 192.168.10.10 \
     --cons_ip 192.168.30.11 \
     --p2cs-listener 192.168.10.11 \
     --c2cs-listener 192.168.30,10 \
     --inbound_ip <local endpoint public ip> \
     --outbound_ip <local endpoint public ip> \
     --inbound-src-ports 5074,5075,5076 \
     --outbound-dst-ports 5100,5101,5102 \
     --num-conn 3
     --rate 10000
     --type StunnelSubprocess
   ```

   What happens:
   - Endpoints resolved & probed
   - Previous session (if any) pre-cleaned (because `--no-deep-clean`)
   - Certs generated + cross-trusted (+ PSK if provided)
   - `s2cs` started on producer/consumer
   - `inbound-request` runs; controller parses UID/ports
   - `outbound-request` runs; connection established

## Logs, PIDs & markers

- **Session dir**: `${session-base}/${session-id}`  
  - `certs/` → `server.crt`, `server.key`, `peer.crt`, optional `psk.secrets`
  - `logs/`  → `p2cs.log`, `c2cs.log`, `inbound.log`, `outbound.log`
  - `procs/` → `*.pid` (actual server PIDs, not wrapper processes)

- **PID dir**: `--pid-dir` (default `/tmp/.scistream`)  
  - `*.pid` written by launchers
  - `.session-<id>.mark` marker used for targeted cleanup

## Implementation notes

- **Controller** (`controller.py`)
  - `_resolve_and_probe_endpoints()` lists endpoints via Globus Compute `Client`, normalizes names, resolves each role, then probes with `echo OK`.
  - Port checks: `_check_remote_ports_free()` executes a small Python script remotely that binds on `0.0.0.0` to detect BUSY ports (fast fail).
  - Certificates: `key_gen()` runs `openssl req -x509` on each gateway; `crt_dist()` copies the peer cert; `key_dist()` writes a PSK.
  - Launch: `launcher.p2cs()` & `launcher.c2cs()` start `s2cs`; `launcher.inbound()` & `launcher.outbound()` run `s2uc` and parse logs.

- **Launchers** (`launcher.py`)
  - Start proxies with **no `timeout` wrapper** so the pidfile captures the `s2cs`/`s2uc` PID.
  - `inbound()` waits until the log contains `prod_listeners:` and extracts the **UID** and **listen ports**.
  - `outbound()` waits for a success marker in the log.

- **Remote exec** (`util.py`)
  - `run_remote()` wraps your script in `bash -c` with `set -euo pipefail`, submits via Globus Compute `Executor/ShellFunction`, and returns `{ok, stdout, stderr}`.
  - `stop_since_marker()` (utility) kills PIDs whose `*.pid` files are **newer than** a given marker file, soft→wait→hard.
