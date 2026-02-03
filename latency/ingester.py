import json
import logging
import subprocess
from typing import Any
import requests


ADMIN_API_BASE_URL = "https://gss.vm.cesnet.cz/gss-admin-api"
INGESTER_VOLUME_NAME = "ingestion-properties"
INGESTER_CONFIG_FILENAME = "database-configuration-for-ingestion.properties"


def run_kubectl_json(cmd: list[str]) -> Any:
    """Run kubectl and parse JSON output."""
    logging.debug(f"Running {" ".join(cmd)}")
    output = subprocess.check_output(cmd, text=True)
    return json.loads(output)


def get_config_file_from_ingester(
    namespace: str,
    label_selector: str,
    volume_name: str = INGESTER_VOLUME_NAME,
    filename: str = INGESTER_CONFIG_FILENAME,
    kubeconfig: str | None = None,
) -> str:
    """
    Resolve ConfigMap backing a given volume from a Deployment selected
    by label and return the content of a static file stored in that ConfigMap.
    """

    # 1️⃣ Get deployments by label
    deploy_cmd = ["kubectl"]
    if kubeconfig:
        deploy_cmd.extend(["--kubeconfig", kubeconfig])
    deploy_cmd.extend([
        "get", "deploy",
        "-n", namespace,
        "-l", label_selector,
        "-o", "json",
    ])
    deployments = run_kubectl_json(deploy_cmd)

    items = deployments.get("items", [])
    if not items:
        raise RuntimeError(f"No Deployment found for selector '{label_selector}'")

    # if len(items) > 1:
    #     raise RuntimeError(
    #         f"Multiple Deployments found for selector '{label_selector}', "
    #         f"refine the selector"
    #     )

    deployment = items[0]
    logging.debug(f"Found deployment {deployment['metadata']['name']} for selector {label_selector}")

    # 2️⃣ Find ConfigMap name for the given volume
    volumes = deployment["spec"]["template"]["spec"].get("volumes", [])
    configmap_name = None

    for vol in volumes:
        if vol.get("name") == volume_name:
            configmap_name = vol["configMap"]["name"]
            break

    if not configmap_name:
        raise RuntimeError(
            f"Volume '{volume_name}' not found in Deployment "
            f"{deployment['metadata']['name']}"
        )

    # 3️⃣ Fetch ConfigMap
    configmap_cmd = ["kubectl"]
    if kubeconfig:
        configmap_cmd.extend(["--kubeconfig", kubeconfig])
    configmap_cmd.extend([
        "get", "configmap", configmap_name,
        "-n", namespace,
        "-o", "json",
    ])
    configmap = run_kubectl_json(configmap_cmd)

    data = configmap.get("data", {})
    if filename not in data:
        raise RuntimeError(
            f"File '{filename}' not found in ConfigMap '{configmap_name}'"
        )

    return data[filename]


def get_first_cdh_producer(config_content: str) -> str | None:
    """
    Extract the first value from cdh.producers in a Java-style properties file.
    Supports comma-separated values.
    Returns the first producer name, or None if not found.
    """

    for line in config_content.splitlines():
        line = line.strip()

        # Skip comments and empty lines
        if not line or line.startswith("#"):
            continue

        if line.startswith("cdh.producers="):
            value = line.split("=", 1)[1].strip()

            if not value:
                return None

            # Split by comma and return the first entry
            return value.split(",", 1)[0].strip()

    return None


def get_session():
    session = requests.Session()
    #session.auth = ("your_username", "your_password"),
    session.auth = None  # use .netrc or no auth
    session.headers.update({"Content-Type": "application/json"})
    return session


def get_producer_entity(producer_name: str) -> dict[str, Any]:
    session = get_session()
    url = f"{ADMIN_API_BASE_URL}/producers/{producer_name}"
    response = session.get(url)
    if response.status_code != 200:
        raise RuntimeError(
            f"Failed to fetch producer '{producer_name}': {response.status_code} {response.text}"
        )

    try:
        return response.json()
    except Exception as exc:
        raise RuntimeError("Invalid JSON response from GSS admin API") from exc


def get_producer_entity_from_product_type(
    namespace: str,
    product_type: str,
    kubeconfig: str | None = None,
):
    config_content = get_config_file_from_ingester(
        namespace=namespace,
        label_selector=f"cdh-ingest.sentinel={product_type}",
        kubeconfig=kubeconfig,
    )
    
    #print("---- configuration file ----")
    #print(config_file_content)
    cdh_producers = get_first_cdh_producer(config_content)
    # print(cdh_producers)
    
    producer_entity = get_producer_entity(cdh_producers)
    return producer_entity


def get_producer_entity_from_product_type_with_variants(
    namespace: str,
    product_type: str,
    kubeconfig: str | None = None,
):
    """Try fetching producer entity using common sentinel label variants."""

    candidates: list[str] = []

    def _add_candidate(value: str):
        if value and value not in candidates:
            candidates.append(value)

    _add_candidate(product_type)
    _add_candidate(product_type.lower())
    _add_candidate(product_type[:-1])
    _add_candidate(product_type[:-1].lower())

    last_exception: Exception | None = None

    for label in candidates:
        logging.debug(f"Trying sentinel label '{label}'")
        try:
            entity = get_producer_entity_from_product_type(
                namespace=namespace,
                product_type=label,
                kubeconfig=kubeconfig,
            )
            logging.debug(f"Found producer entity using label '{label}'")
            return entity
        except Exception as exc:  # noqa: BLE001 - surface root errors and try next variant
            logging.debug(f"Attempt to find producer with label '{label}' failed: {exc}")
            last_exception = exc

    raise RuntimeError(
        f"Could not fetch producer entity for product type '{product_type}' "
        f"using candidates {candidates}"
    ) from last_exception


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    producer_entity = get_producer_entity_from_product_type_with_variants(
        namespace="relay",
        product_type="S2A"
    )

    print(json.dumps(producer_entity, indent=2))
