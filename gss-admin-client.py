#!/usr/bin/env python3

import argparse
import json
import requests
import logging

# GSS admin OpenAPI definition https://<your-gss-site.esa.int>/gss-admin-api/v3/api-docs
ENTITIES = [
    "datastores/hfs", "consumers", "producers", "quotas", 
    "metadatastores/solr", "datastores/timebased", "swiftcredentials", 
    "jobs", "datastores/swiftgroup", "datastores/swift"
]
BASE_URL = "https://collgs.cesnet.cz/gss-admin-api"

def load_json_file(file_path):
    with open(file_path, "r") as f:
        return json.load(f)

def get_session():
    session = requests.Session()
    #session.auth = ("your_username", "your_password"),
    session.auth = None  # use .netrc or no auth
    session.headers.update({"Content-Type": "application/json"})
    return session
    
def print_entry(entry, pretty):
    if pretty:
        print(json.dumps(entry, sort_keys=True, indent=4))
    else:
        print(json.dumps(entry))

# The processes and displays JSON responses so we can save&edit later
def print_entries(response, entity, pretty):
    try:
        data = response.json()
        if isinstance(data, dict) and entity in data:
            for key, entries in data.items():
                if isinstance(entries, list):
                    for entry in entries:
                        print_entry(entry, pretty)
        elif isinstance(data, list):
            for entry in data:
                print_entry(entry, pretty)
        else:
            print_entry(data, pretty)
    except json.JSONDecodeError:
        logging.error("Invalid JSON response")
        print(response.text)

def handle_request(session, entity, action, entity_name=None, data=None, pretty=False):
    url = f"{BASE_URL}/{entity}"
    
    if action == "get":
        if entity_name:
            url = f"{url}/{entity_name}"
        response = session.get(url)
    elif action == "create" and data:
        response = session.post(url, json=data)
    elif action == "update" and data:
        entity_name = data.get('name')
        if not entity_name:
            raise Exception("Error: 'name' field is required in JSON data for update action")
        response = session.patch(f"{url}/{entity_name}", json=data)
    elif action == "delete" and entity_name:
        response = session.delete(f"{url}/{entity_name}")
    elif action == "dump":
        dump_all_entities(session, ENTITIES, pretty)
        return
    else:
        logging.error("Invalid action or missing parameters")
        return
    
    logging.debug(f"# HTTP status code {response.status_code}")
    print_entries(response, entity, pretty)

def dump_all_entities(session, entities, pretty):
    for entity in entities:
        url = f"{BASE_URL}/{entity}"
        response = session.get(url)
        logging.debug(f"# {entity}: {response.status_code}")
        print_entries(response, entity, pretty)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GSS Admin API Console Client")
    parser.add_argument("action", choices=["get", "create", "update", "delete", "dump"], help="Action to perform")
    parser.add_argument("entity", choices=ENTITIES, nargs="?", help="Entity type")
    parser.add_argument("--name", "-n", type=str, help="Name of entity to operate on")
    parser.add_argument("--data-file", "-f", type=str, help="Path to JSON file with data for create/update actions")
    parser.add_argument("-d", action="count", default=0, help="Increase logging verbosity (-d: INFO, -dd: DEBUG)")
    parser.add_argument("--pretty", "-p", action="store_true", help="Pretty print JSON output")
    
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
    
    session = get_session()

    data = None
    if args.data_file:
        try:
            data = load_json_file(args.data_file)
        except Exception as e:
            logging.error(f"Error loading JSON file: {e}")
    
    handle_request(session, args.entity, args.action, args.name, data, args.pretty)
    
