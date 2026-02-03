import json
import logging
import subprocess
from typing import Any
import requests


class Ingester:

    DEFAULT_ADMIN_API_BASE_URL = "https://gss.admin-api.site/gss-admin-api"
    DEFAULT_INGESTER_VOLUME_NAME = "ingestion-properties"
    DEFAULT_INGESTER_CONFIG_FILENAME = "database-configuration-for-ingestion.properties"

    def __init__(
        self,
        namespace: str | None = None,
        admin_api_base_url: str = DEFAULT_ADMIN_API_BASE_URL,
        admin_api_user: str | None = None,
        admin_api_password: str | None = None,
        volume_name: str = DEFAULT_INGESTER_VOLUME_NAME,
        config_filename: str = DEFAULT_INGESTER_CONFIG_FILENAME,
        kubeconfig: str | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.namespace = namespace
        self.admin_api_base_url = admin_api_base_url
        self.admin_api_user = admin_api_user
        self.admin_api_password = admin_api_password
        self.volume_name = volume_name
        self.config_filename = config_filename
        self.kubeconfig = kubeconfig
        self._session = session

    def run_kubectl_json(self, cmd: list[str]) -> Any:
        """Run kubectl and parse JSON output."""
        cmd_str = " ".join(cmd)
        logging.debug(f"Running {cmd_str}")
        output = subprocess.check_output(cmd, text=True)
        return json.loads(output)

    def get_config_file_from_ingester(
        self,
        label_selector: str,
    ) -> str:
        """
        Resolve ConfigMap backing a given volume from a Deployment selected
        by label and return the content of a static file stored in that ConfigMap.
        """
        if not self.namespace:
            raise RuntimeError("Ingester namespace is not configured")

        deploy_cmd = ["kubectl"]
        if self.kubeconfig:
            deploy_cmd.extend(["--kubeconfig", self.kubeconfig])
        deploy_cmd.extend([
            "get", "deploy",
            "-n", self.namespace,
            "-l", label_selector,
            "-o", "json",
        ])
        deployments = self.run_kubectl_json(deploy_cmd)

        items = deployments.get("items", [])
        if not items:
            raise RuntimeError(f"No Deployment found for selector '{label_selector}'")

        deployment = items[0]
        logging.debug(
            f"Found deployment {deployment['metadata']['name']} for selector {label_selector}"
        )

        volumes = deployment["spec"]["template"]["spec"].get("volumes", [])
        configmap_name = None
        for vol in volumes:
            if vol.get("name") == self.volume_name:
                configmap_name = vol["configMap"]["name"]
                break

        if not configmap_name:
            raise RuntimeError(
                f"Volume '{self.volume_name}' not found in Deployment "
                f"{deployment['metadata']['name']}"
            )

        configmap_cmd = ["kubectl"]
        if self.kubeconfig:
            configmap_cmd.extend(["--kubeconfig", self.kubeconfig])
        configmap_cmd.extend([
            "get", "configmap", configmap_name,
            "-n", self.namespace,
            "-o", "json",
        ])
        configmap = self.run_kubectl_json(configmap_cmd)

        data = configmap.get("data", {})
        if self.config_filename not in data:
            raise RuntimeError(
                f"File '{self.config_filename}' not found in ConfigMap '{configmap_name}'"
            )

        return data[self.config_filename]

    @staticmethod
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

    def get_session(self) -> requests.Session:
        if self._session is None:
            session = requests.Session()
            if self.admin_api_user is not None and self.admin_api_password is not None:
                session.auth = (self.admin_api_user, self.admin_api_password)
            else:
                session.auth = None  # use .netrc or no auth
            session.headers.update({"Content-Type": "application/json"})
            self._session = session
        return self._session

    def get_producer_entity(self, producer_name: str) -> dict[str, Any]:
        if not producer_name:
            raise RuntimeError("Missing producer name in ingestion configuration")

        session = self.get_session()
        url = f"{self.admin_api_base_url}/producers/{producer_name}"
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
        self,
        product_type: str,
    ) -> dict[str, Any]:
        config_content = self.get_config_file_from_ingester(
            label_selector=f"cdh-ingest.sentinel={product_type}",
        )
        cdh_producer = self.get_first_cdh_producer(config_content)
        return self.get_producer_entity(cdh_producer)

    def get_producer_entity_from_product_type_with_variants(
        self,
        product_type: str,
    ) -> dict[str, Any]:
        """Try fetching producer entity using common sentinel label variants."""
        candidates: list[str] = []

        def _add_candidate(value: str) -> None:
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
                entity = self.get_producer_entity_from_product_type(
                    product_type=label,
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
    
    _default_ingester = Ingester(namespace="relay")
    producer_entity = _default_ingester.get_producer_entity_from_product_type_with_variants(
        product_type="S2A"
    )

    print(json.dumps(producer_entity, indent=2))
