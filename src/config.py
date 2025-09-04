from __future__ import annotations
import argparse

def _csv_ports(s: str) -> list[int]:
    if not s:
        return []
    try:
        return [int(p) for p in s.split(",") if p.strip()]
    except ValueError:
        raise argparse.ArgumentTypeError("Ports must be comma-separated integers")

def get_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SciStream Controller")

    g_ep = p.add_argument_group("Endpoints")
    g_ep.add_argument("--p2cs-ep", default="thats", help="Producer-side endpoint name")
    g_ep.add_argument("--c2cs-ep", default="neat", help="Consumer-side endpoint name")
    g_ep.add_argument("--inbound-ep", default="swell", help="Endpoint name for running s2uc inbound-request")
    g_ep.add_argument("--outbound-ep", default="swell", help="Endpoint name for running s2uc outbound-request")

    g_net = p.add_argument_group("Network/Listeners")
    g_net.add_argument("--p2cs_ip", default="128.135.164.119")
    g_net.add_argument("--c2cs_ip", default="128.135.164.120")
    g_net.add_argument("--prod_ip", default="128.135.24.117")
    g_net.add_argument("--cons_ip", default="128.135.24.118")
    g_net.add_argument("--p2cs-listener", default="128.135.24.119")
    g_net.add_argument("--c2cs-listener", default="128.135.24.120")
    g_net.add_argument('--inbound_ip', help='inbound IP address', default='128.135.24.118')
    g_net.add_argument('--outbound_ip', help='outbound IP address', default='128.135.24.118')
    g_net.add_argument("--type", default="StunnelSubprocess", help="Type of proxy to use")
    g_net.add_argument("--sync-port", type=int, default=5000)
    g_net.add_argument("--rate", type=int, default=10_000)
    g_net.add_argument("--num-conn", type=int, default=11)
    g_net.add_argument("--inbound-src-ports", type=_csv_ports, default="5074,5075,5076,5077,5078,5079,5080,5081,5082,5083,5084")
    g_net.add_argument("--outbound-dst-ports", type=_csv_ports, default="5050,5100,5101,5102,5103,5104,5105,5106,5107,5108,5109,5110")

    g_exec = p.add_argument_group("Launch Commands")
    g_exec.add_argument("--p2cs-cmd", default="", help="Remote shell to launch p2cs (optional)")
    g_exec.add_argument("--c2cs-cmd", default="", help="Remote shell to launch c2cs (optional)")
    g_exec.add_argument("--inbound-cmd", default="", help="Remote shell to init inbound (optional)")
    g_exec.add_argument("--outbound-cmd", default="", help="Remote shell to init outbound (optional)")

    g_sec = p.add_argument_group("Security")
    g_sec.add_argument("--psk-secret", default="", help="Optional PSK secret; if empty, skip PSK dist")

    g_paths = p.add_argument_group("Paths")
    g_paths.add_argument("--session-base", default="/tmp/.scistream")
    g_paths.add_argument("--pid-dir", default="/tmp/.scistream", help="Where SciStream writes .pid files")

    g_flags = p.add_argument_group("Flags")
    g_flags.add_argument("--no-cleanup", action="store_true", help="Skip cleanup at the end")
    g_flags.add_argument("-v", "--verbose", action="store_true")

    """
    g_paths = p.add_argument_group("Paths / Flags")
    g_paths.add_argument("--session-base", default="/tmp/.scistream")
    g_paths.add_argument("--pid-dir", default="~/.scistream")
    g_paths.add_argument("--no-cleanup", action="store_true")
    g_paths.add_argument("-v", "--verbose", action="store_true")
    """

    args = p.parse_args()
    if not (1 <= args.sync_port <= 65535):
        p.error("--sync-port must be 1..65535")
    return args