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

    g_net = p.add_argument_group("Network IPs and Ports")
    g_net.add_argument("--p2cs_ip", default="128.135.164.119")
    g_net.add_argument("--c2cs_ip", default="128.135.164.120")
    g_net.add_argument("--prod_ip", default="128.135.24.117")
    g_net.add_argument("--cons_ip", default="128.135.24.118")
    g_net.add_argument("--p2cs-listener", default="128.135.24.119")
    g_net.add_argument("--c2cs-listener", default="128.135.24.120")
    g_net.add_argument('--inbound_ip', help='inbound IP address', default='128.135.24.118')
    g_net.add_argument('--outbound_ip', help='outbound IP address', default='128.135.24.118')
    g_net.add_argument("--inbound-src-ports", type=_csv_ports, default="5074,5075,5076,5077,5078,5079,5080,5081,5082,5083,5084")
    g_net.add_argument("--outbound-dst-ports", type=_csv_ports, default="5050,5100,5101,5102,5103,5104,5105,5106,5107,5108,5109,5110")

    g_general = p.add_argument_group("General")
    g_general.add_argument("--type", default="StunnelSubprocess", help="Type of proxy to use: StunnelSubprocess, HaproxySubprocess")
    g_general.add_argument("--sync-port", type=int, default=5000)
    g_general.add_argument("--rate", type=int, default=10_000)
    g_general.add_argument("--num-conn", type=int, default=11)
    g_general.add_argument("--psk-secret", default="", help="Optional PSK secret; if empty, skip PSK dist")

    g_paths = p.add_argument_group("Paths")
    g_paths.add_argument("--session-base", default="/tmp/.scistream")
    g_paths.add_argument("--pid-dir", default="/tmp/.scistream", help="Where .pid files are stored")

    g_flags = p.add_argument_group("Flags")
    g_flags.add_argument("--cleanup", action="store_true", help="Cleanup the connections from the previous session (if any) before starting a new one")
    g_flags.add_argument("--no-deep-clean", action='store_true', default=False, help="Cleanup all the connections from the previous session (if any) before starting a new one")
    g_flags.add_argument("-v", "--verbose", action="store_true")

    args = p.parse_args()
    if not (1 <= args.sync_port <= 65535):
        p.error("--sync-port must be 1..65535")
    return args