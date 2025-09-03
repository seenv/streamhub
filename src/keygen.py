import logging, sys
from globus_compute_sdk import Executor, ShellFunction
import re


#TODO:  chmod 600 private       chmod 644 public

def key_gen(args, endpoint_name, uuid):
    """Start the key generation and distribution."""

    cmd =   f"""
            bash -c '
            [[ -z "$HAPROXY_CONFIG_PATH" ]] && HAPROXY_CONFIG_PATH="/tmp/.scistream" && mkdir -p "$HAPROXY_CONFIG_PATH"
            openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout $HAPROXY_CONFIG_PATH/server.key -out $HAPROXY_CONFIG_PATH/server.crt \
                -subj "/CN={args.p2cs_listener}" -addext "subjectAltName=IP:{args.p2cs_ip}, IP:{args.prod_ip}, IP:{args.c2cs_listener}, IP:{args.c2cs_ip}, IP:{args.cons_ip}, IP:{args.inbound_ip}, IP:{args.outbound_ip}" 2>/dev/null
            sleep 5
            cat "$HAPROXY_CONFIG_PATH/server.crt" "$HAPROXY_CONFIG_PATH/server.key" > "$HAPROXY_CONFIG_PATH/stream.pem"; chmod 600 "$HAPROXY_CONFIG_PATH/stream.pem"
            cat "$HAPROXY_CONFIG_PATH/stream.pem"
            
            '
            """

    _KEY_RE = re.compile(
        r"(-----BEGIN PRIVATE KEY-----.+?-----END PRIVATE KEY-----)",
        re.DOTALL
    )
    _CRT_RE = re.compile(
        r"(-----BEGIN CERTIFICATE-----.+?-----END CERTIFICATE-----)",
        re.DOTALL
    )
    
    with Executor(endpoint_id=uuid) as gce:

        print(f"Starting the key generation: \n"
              f"    endpoint: {endpoint_name.capitalize()} \n"
              f"    endpoint uid: {uuid} \n"
              f"\n")
        logging.info(f"KeyGen: Starting KeyGen on endpoint ({endpoint_name.capitalize()}) with args: \n{args}")
        future = gce.submit(ShellFunction(cmd, walltime=60))

        try:
            results = future.result(timeout=60)
            
            pem = results.stdout
            logging.info(f"KeyGen Output: {pem}")

            key_match = _KEY_RE.search(pem)
            crt_match = _CRT_RE.search(pem)

            key = key_match.group(1) if key_match else None
            crt = crt_match.group(1) if crt_match else None
            
            logging.info(f"Key and CRT contents:{key} \n {crt}")
            return key, crt

        except Exception as e:
            logging.error(f"KeyGen Exception: {e}")
            print(f"The KeyGen failed due to the following Exception: {e} \n")
            sys.exit(1)
        
        print(f"The Key Generation is completed on the endpoint {endpoint_name.capitalize()} \n")
        logging.info(f"KeyGen: The Key Generation is completed on the endpoint {endpoint_name.capitalize()}")




def key_dist(args, endpoint_name, uuid, key, crt):
    """Start the key distribution."""

    if not key and not crt:
        print(f"The Key and Crt are not generated......")
        logging.error(f"KeyDist: The Key and Crt are not generated......")
        return

    cmd =   f"""
            bash -c '
            [[ -z "$HAPROXY_CONFIG_PATH" ]] && HAPROXY_CONFIG_PATH="/tmp/.scistream" && mkdir -p "$HAPROXY_CONFIG_PATH"
            sleep 1
            echo "{crt}" > "$HAPROXY_CONFIG_PATH/server.crt"
            echo "{key}" > "$HAPROXY_CONFIG_PATH/server.key"
            cat "$HAPROXY_CONFIG_PATH/server.crt" "$HAPROXY_CONFIG_PATH/server.key" > "$HAPROXY_CONFIG_PATH/stream.pem"; chmod 600 "$HAPROXY_CONFIG_PATH/stream.pem"
            cat "$HAPROXY_CONFIG_PATH/stream.pem"
            '
            """

    with Executor(endpoint_id=uuid) as gce:

        print(f"Distributing the Key and Certificate to the S2CS: \n"
              f"    endpoint: {endpoint_name.capitalize()} \n"
              f"    endpoint uid: {uuid} \n"
              f"\n")
        logging.info(f"Distributing the Key and Certificate to the S2CS endpoint ({endpoint_name.capitalize()}) with args: \n{args} \n")
        future = gce.submit(ShellFunction(cmd), walltime=60)

        try:
            result = future.result(timeout=60)
            logging.info(f"KeyDist Output: {result.stdout}")

        except Exception as e:
            logging.error(f"KeyDist Exception: {e}")
            print(f"The Key Distribution failed due to the following Exception: {e}")
            sys.exit(1)

        print(f"The Key Distribution is completed on the endpoint {endpoint_name.capitalize()} \n")
        logging.info(f"KeyDist: The Key Distribution is completed on the S2CS endpoint {endpoint_name.capitalize()}")
        #gce.shutdown(wait=True, cancel_futures=False)
    



def crt_dist(args, endpoint_name, uuid, crt):
    """Start the key distribution."""

    if not crt:
        print(f"The Key and Crt are not generated......")
        logging.error(f"KeyDist: The Key and Crt are not generated......")
        return

    cmd =   f"""
            bash -c '
            [[ -z "$HAPROXY_CONFIG_PATH" ]] && HAPROXY_CONFIG_PATH="/tmp/.scistream" && mkdir -p "$HAPROXY_CONFIG_PATH"
            sleep 1
            echo "{crt}" > "$HAPROXY_CONFIG_PATH/server.crt"
            '
            """
    
    with Executor(endpoint_id=uuid) as gce:

        print(f"Distributing the Certificate to the endpoint: \n"
              f"    endpoint: {endpoint_name.capitalize()} \n"
              f"    endpoint uid: {uuid} \n"
              f"\n")
        logging.info(f"Distributing the Certificate to the endpoint ({endpoint_name.capitalize()}) with args: \n{args} \n")
        future = gce.submit(ShellFunction(cmd), walltime=60)

        try:
            result = future.result(timeout=60)
            logging.info(f"KeyDist Output: {result.stdout}")

        except Exception as e:
            logging.error(f"CertDist Exception: {e}")
            print(f"The Cert Distribution failed due to the following Exception: {e}")
            sys.exit(1)

        print(f"The Cert Distribution is completed on the endpoint {endpoint_name.capitalize()} \n")
        logging.info(f"CertDist: The Cert Distribution is completed on the endpoint {endpoint_name.capitalize()}")
        #gce.shutdown(wait=True, cancel_futures=False)