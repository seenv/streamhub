from __future__ import annotations
import logging
import signal
import sys

from config import get_args
from controller import StreamController


def _install_signal_cleanup(ctl: StreamController):
    def handler(sig, frame):
        logging.warning("Received signal %s; running cleanup...", sig)
        try:
            res = ctl.cleanup()
            logging.info("Cleanup (signal): %s", res)
        finally:
            sys.exit(1)
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


def main():
    args = get_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    ctl = StreamController(args)
    _install_signal_cleanup(ctl)

    # 1) Markers inside the real SciStream PID dir (e.g., ~/.scistream)
    mark = ctl.create_remote_markers()
    if any(not r.get("ok") for r in mark.values()):
        logging.error("Failed to create session markers: %s", mark)
        sys.exit(2)
    
    # 2) Crypto: keygen on both ends + cross-trust
    crypto = ctl.setup_crypto()
    if any(not r.get("ok") for r in crypto.values()):
        logging.error("Crypto setup failed: %s", crypto)
        sys.exit(2)

    # 3) Optional PSK distribution
    psk = ctl.distribute_psk()
    if any(not r.get("ok") for r in psk.values()):
        logging.error("PSK distribution failed: %s", psk)
        sys.exit(2)

    # 4) Launch p2cs and c2cs
    r1 = ctl.launch_p2cs()
    r2 = ctl.launch_c2cs()
    if not r1.get("ok") or not r2.get("ok"):
        logging.error("Service launch failed: p2cs=%s c2cs=%s", r1, r2)
        res = ctl.cleanup()
        logging.info("Cleanup after launch failure: %s", res)
        sys.exit(3)

    # 5) Connect: inbound → parse UID/ports → outbound
    conn = ctl.connect()
    if any(not v.get("ok") for v in conn.values()):
        logging.error("Connect failed: %s", conn)
        res = ctl.cleanup()
        logging.info("Cleanup after connect failure: %s", res)
        sys.exit(4)

    # 6) Finish. Unless user opted out, clean up what this session started.
    if args.no_cleanup:
        logging.info("Started successfully; skipping cleanup due to --no-cleanup")
        return

    res = ctl.cleanup()
    logging.info("Cleanup: %s", res)


if __name__ == "__main__":
    main()