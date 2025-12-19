#!/usr/bin/env python3
import subprocess
import requests
import json
import logging
import check_product_counts


def get_endpoint_ip(namespace, service):
    # Get the Endpoints object in JSON
    cmd = ["kubectl", "get", "endpoints", service, "-n", namespace, "-o", "json"]
    ep_json = subprocess.check_output(cmd, text=True)
    eps = json.loads(ep_json)

    # Extract IPs + ports
    subsets = eps.get("subsets", [])
    if not subsets:
        raise RuntimeError("No endpoints found")

    addresses = subsets[0].get("addresses", [])
    ports = subsets[0].get("ports", [])

    if not addresses or not ports:
        raise RuntimeError("No ready addresses or ports in endpoint")

    ip = addresses[0]["ip"]
    port = ports[0]["port"]
    return f"{ip}:{port}"

def parse_kafka(output: str):
    metrics = []
    for line in output.splitlines():
        # skip headers and empty lines
        if not line.strip():
            continue
        if line.startswith("GROUP") or line.startswith("group"):
            continue

        parts = line.split()
        if len(parts) < 7:
            continue

        group = parts[0]
        topic = parts[1]
        partition = parts[2]
        current_offset = parts[3]
        logend_offset = parts[4]
        lag = parts[5]
        client_id = parts[6]

        #labels = f'group="{group}",topic="{topic}",partition="{partition}",client="{client_id}"'
        labels = f'group="{group}",topic="{topic}",partition="{partition}"'

        metrics.append(f'kafka_consumer_offset{{{labels}}} {current_offset}')
        metrics.append(f'kafka_logend_offset{{{labels}}} {logend_offset}')
        metrics.append(f'kafka_consumer_lag{{{labels}}} {lag}')

    return metrics


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    # ---- Config ----
    # http://vms-victoria-metrics-single-server.monitoring.svc.cluster.local.:8428
    VM_HOST = get_endpoint_ip('monitoring', 'vms-victoria-metrics-single-server')
    VICTORIA_URL = f"http://{VM_HOST}/api/v1/import/prometheus"
    COMMAND = ["/root/bin/kafka-get-queue-informations.sh", "relay", "-q"]

    GSS_URL = "https://gss.vm.cesnet.cz/odata/v1"

    metrics = []

    # Run the command and capture output
    proc = subprocess.run(COMMAND, capture_output=True, text=True, check=True)
    metrics += parse_kafka(proc.stdout)
    metrics += check_product_counts.gss_metrics(GSS_URL)
    
    metrics_text = "\n".join(metrics) + "\n"
    
    logging.debug(VICTORIA_URL)
    logging.debug(metrics_text)

    # Push to VictoriaMetrics
    resp = requests.post(VICTORIA_URL, data=metrics_text.encode("utf-8"))
    resp.raise_for_status()
    logging.debug(f"Pushed metrics successfully: {resp.text}")

