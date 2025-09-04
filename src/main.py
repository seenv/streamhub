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
        format="%(asctime)s %(levelname)s %(message)s",
        #handlers=[
        #    logging.FileHandler("streamhub.log"),
        #]
    )

    ctl = StreamController(args)
    _install_signal_cleanup(ctl)

    #  Cleanup previous session's proxies
    if args.cleanup:
        pre = ctl.preclean_previous_session()
        logging.info("Pre-clean (previous session): %s", pre)
        if any(not r.get("ok") for r in pre.values()):
            logging.error("Pre-clean failed: %s", pre)
            sys.exit(2)
    
    #  Cleanup all the proxies
    if args.deep_clean:
        deep = ctl.deep_clean_previous_session()
        logging.info("Deep-clean: %s", deep)
        if any(not r.get("ok") for r in deep.values()):
            logging.error("Deep-clean failed: %s", deep)
            sys.exit(2)

    # Fail if requested ports are busy
    try:
        ctl.verify_requested_ports_available()
    except Exception as e:
        logging.error("Port availability check failed: %s", e)
        sys.exit(2)

    # Markers inside the real SciStream PID dir (/tmp/.scistream/{sess})
    mark = ctl.create_remote_markers()
    if any(not r.get("ok") for r in mark.values()):
        logging.error("Failed to create session markers: %s", mark)
        sys.exit(2)

    # Crypto: keygen on both ends + cross-trust
    crypto = ctl.setup_crypto()
    if any(not r.get("ok") for r in crypto.values()):
        logging.error("Crypto setup failed: %s", crypto)
        sys.exit(2)

    # PSK distribution
    psk = ctl.distribute_psk()
    if any(not r.get("ok") for r in psk.values()):
        logging.error("PSK distribution failed: %s", psk)
        sys.exit(2)

    # Launch p2cs and c2cs
    r1 = ctl.launch_p2cs()
    r2 = ctl.launch_c2cs()
    if not r1.get("ok") or not r2.get("ok"):
        logging.error("Service launch failed: p2cs=%s c2cs=%s", r1, r2)
        res = ctl.cleanup()
        logging.info("Cleanup after launch failure: %s", res)
        sys.exit(3)

    # Connect: inbound → parse UID/ports → outbound
    conn = ctl.connect()
    if any(not v.get("ok") for v in conn.values()):
        logging.error("Connect failed: %s", conn)
        res = ctl.cleanup()
        logging.info("Cleanup after connect failure: %s", res)
        sys.exit(4)


if __name__ == "__main__":
    main()