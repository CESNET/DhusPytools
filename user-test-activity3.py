import requests
import random
import logging
import os
import argparse
import HTTPAuthOptions

# OData service base URL
#BASE_URL = "https://dhr1.cesnet.cz/odata/v2"
#BASE_URL = "https://gss.dhr.metacentrum.cz/odata/v1"
#BASE_URL = "https://dhs2.copernicus.eu/odatav4/odata/v2"
BASE_URL = "https://collgs.cesnet.cz/odata/v1"

# Keycloak authentication data
TOKEN_URL="https://dhs2.copernicus.eu/auth"
REALM = "gss"
CLIENT_ID="dhs2"

# Destination directory for downloads
DOWNLOAD_DIR = "./tmp/"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

MAX_PRODUCTS = 2

nodes_to_url = lambda node_ids: "/".join([f"Nodes('{node_id}')" for node_id in node_ids])

def get_products(auth, queries):
    """Fetch products by given queries."""
    products_by_query = {}

    for query in queries:
        response = requests.get(
            f"{BASE_URL}/Products?{query}",
            auth=auth
        )

        if response.status_code == 200:
            data = response.json()
            products = data.get("value", [])

            if products:
                products_by_query[query] = random.sample(products, min(MAX_PRODUCTS, len(products)))
                logging.info(f"Found {len(products_by_query[query])} products for type {query}.")
            else:
                logging.warning(f"No products found for type {query}.")
        else:
            logging.error(f"Failed to fetch products for {query}: {response.status_code} {response.text}")

    return products_by_query

def download_value(entity, entity_id, auth, entity_type, node_ids=None):
    """Download entity's $value (binary content) to tmp."""
    if entity_type == 'Nodes':
        url = f"{BASE_URL}/Products({entity_id})/{nodes_to_url(node_ids)}/$value"
    else:
        url = f"{BASE_URL}/{entity_type}({entity_id})/$value"
    response = requests.get(url, auth=auth, stream=True)

    if response.status_code == 200:
        file_path = os.path.join(DOWNLOAD_DIR, f"{entity['Name']}")
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logging.info(f"Downloaded {entity_type} {entity['Id']} to {file_path}")
    else:
        logging.error(f"Failed to download {entity_type} {entity['Id']} value: {response.status_code} {requests.status_codes._codes[response.status_code][0]}")

def inspect_nodes(auth, product_id, node_id, depth=0, max_depth=1):
    """Recursively explore Nodes and download some of their $value."""
    if depth > max_depth:
        return

    logging.info(f"Inspecting Node {node_id}, Depth {depth}")

    node_entity_response = requests.get(
        f"{BASE_URL}/Products({product_id})/{nodes_to_url(node_id)}?$format=json",
        auth=auth
    )
    if node_entity_response.status_code == 200:
        node_entity = node_entity_response.json()
        if node_entity:
            logging.info(f"  Node entity found for Node {node_id}.")
            inspect_child_nodes(auth, product_id, node_id, depth, max_depth)
        else:
            logging.warning(f"  No node entity found for Node {node_id}.")
    else:
        logging.error(f"  Failed to fetch node entity for Node {node_id}: {node_entity_response.status_code}")

# Fetch child nodes
def inspect_child_nodes(auth, product_id, node_ids, depth=0, max_depth=1):
    node_response = requests.get(
        f"{BASE_URL}/Products({product_id})/{nodes_to_url(node_ids)}/Nodes?$format=json",
        auth=auth
    )

    if node_response.status_code == 200:
        nodes = node_response.json().get("value", [])
        if nodes:
            logging.info(f"  Found {len(nodes)} child nodes for Node {node_ids}")

            # Randomly select a few nodes to download
            selected_nodes = random.sample(nodes, min(2, len(nodes)))
            for node in selected_nodes:
                node_id = node["Id"]
                #download_value(node, product_id, auth, "Nodes", node_ids)

                # Recursively go deeper
                inspect_nodes(auth, product_id, node_ids + [node_id], depth + 1, max_depth)
        else:
            logging.warning(f"  No child nodes found for Node {node_ids}")
    else:
        logging.error(f"  Failed to fetch nodes for Node {node_ids}: {node_response.status_code}")

def inspect_products(auth, products_by_query):
    """Fetch and log attributes, nodes, and download $value for selected products."""
    for query, products in products_by_query.items():
        logging.info(f"Inspecting Product Result: {query}")

        for product in products:
            product_id = product["Id"]
            product_name = product["Name"]
            logging.info(f"Product ID: {product_id}, Name: {product_name}")

            # Download product $value
            download_value(product, product_id, auth, "Products")

            # Get attributes
            attr_response = requests.get(
                f"{BASE_URL}/Products({product_id})/Attributes?$format=json",
                auth=auth
            )
            if attr_response.status_code == 200:
                attributes = attr_response.json().get("value", [])
                if attributes:
                    logging.info(f"  Attributes found for Product {product_id}.")
                else:
                    logging.warning(f"  No attributes found for Product {product_id}.")
            else:
                logging.error(f"  Failed to fetch attributes for Product {product_id}: {attr_response.status_code}")

            # Get nodes
            node_response = requests.get(
                f"{BASE_URL}/Products({product_id})/Nodes?$format=json",
                auth=auth
            )
            if node_response.status_code == 200:
                nodes = node_response.json().get("value", [])
                if nodes:
                    logging.info(f"Found {len(nodes)} nodes for Product {product_id}")

                    # Select a random node and walk deeper
                    random_node = random.choice(nodes)
                    inspect_nodes(auth, product_id, [random_node["Id"]])
                else:
                    logging.warning(f"No nodes found for Product {product_id}")
            else:
                logging.error(f"  Failed to fetch nodes for Product {product_id}: {node_response.status_code}")



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    # Create a mutually exclusive group
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-b", action="store_true", help="Use basic authentication (.basic-auth file)")
    group.add_argument("-t", action="store_true", help="Use token authentication (.token file)")
    group.add_argument("-k", action="store_true", help="Use keycloak authentication (.basic-auth file)")
    
    parser.add_argument("-d", action="count", default=0, help="Increase logging verbosity (-d: INFO, -dd: DEBUG)")
    
    # Parse arguments
    args = parser.parse_args() 
    
    # Set logging level based on occurrences of -d
    if args.d >= 2:
        log_level = logging.DEBUG
    elif args.d == 1:
        log_level = logging.INFO
    else:
        log_level = logging.WARNING

    # Configure logging
    logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s - %(message)s")

    if args.k:
        auth = HTTPAuthOptions.KeycloakTokenAuth(server_url=TOKEN_URL, realm=REALM, client_id=CLIENT_ID)
    elif args.t:
        auth = HTTPAuthOptions.HTTPBearerAuth()
    else:
        #auth = HTTPAuthOptions.FileBasedBasicAuth()
        auth = None

    with open("filters.txt", "r") as file:
        lines = file.readlines()
    queries = [line.strip() for line in lines if line.strip() and not line.lstrip().startswith("#")]
    
    logging.info("Starting OData queries...")
    products_by_query = get_products(auth, queries)
    inspect_products(auth, products_by_query)

    logging.info("OData queries completed successfully.")
