# StreamHub

StreamHub is an orchestrated, secure, and high-performance framework for configuring and running streaming endpoints. It integrates automated system setup (via Ansible) with a Python control layer to launch and manage data streams. StreamHub is designed for distributed, high-throughput scientific applications where reproducibility, security, and efficiency are essential.

---

## ✨ Features

### Endpoint Orchestration
- Automated installation and configuration of dependencies (Docker, Python, firewalls, sysctl tuning).
- Secure tunneling with support for **Stunnel** and **HAProxy**.

### Research Integration
- Deploys and configures [SciStream](https://github.com/scistream/scistream-proto) on endpoints.
- Supports **Globus Compute** endpoints for federated orchestration.

---

## Requirements

### System Requirements
- Ubuntu 22.04 or later
- Python 3.9+
- SSH access to endpoints
- At least 4 GB memory and 10 GB disk space

### Installed Automatically
- Ansible
- Docker & Docker Compose
- Python venv and scientific libraries
- Stunnel, HAProxy, Nginx, iPerf3, tshark

---

## Installation

### 1. Clone the Repository
```bash
git clone https://github.com/<your-org>/streamhub.git
cd streamhub
```

### 2. Run the Bootstrap Script
```bash
./bootstrap.sh
```
**Note:** you can ignore the setup tasks that are not required, such as firewall or sysctl (have to be updated in case they are required).

This will:
- Install **Ansible** (if missing)
- Apply **site.yml** to configure the endpoint

### 3. Configure Hosts
Edit the inventory file:
```bash
nano setup/inventory/hosts
```
Add your producer and consumer nodes.

---
## Usage

### Run StreamHub
```bash
python3 src/main.py
```

This will:
- Check the user endpoints
- Stops any HAProxy or Stunnel proxies to avoid conflict
- Starts the SciStream's S2CS (Control Server) on the gateway endpoints
- Starts the SciStream's S2UC (User Control) on the inbound/outbound initiator endpoint, and
- Initiates the tunnel,and finally,
- Starts SciStream's S2DS (Data server) on the gateways to stream the data from producer to the consumer.

### Test the stream with iPerf3:
- On the Producer start iPerf's server:
```iperf3 -s -p <PORT-NUMBER>      #default ports are defined on the configuration file in "src/config.py"```
- On the Consumer endpoint start iPerf's client:
```iperf3 -c <C2CS's-LOCAL-IPV4> -p <C2CS's-LISTENING-PORT>```
For example if the IP addresses are like:
```
Producer:
  local:   192.168.10.10
P2CS: (Producers gateway)
  local:   192.168.10.11
  public:  192.168.20.10
C2CS: (Consumers gateway)
  local:   192.168.30.10
  public:  192.168.20.11
Consumer:
  local:   192.168.30.11
```

Then, on the server run:
```iperf3 -s -p 5074```
and on the client:
```iperf3 -c 192.168.30.10 -p 5100```
---

## Acknowledgments

- [**SciStream**](https://github.com/scistream/scistream-proto) – Secure streaming framework for scientific data.
- [**Globus Compute**](https://www.globus.org/compute) – Federated orchestration of distributed tasks.
- **APS Mini-App** – Reference workloads for streaming evaluation.
