#!/usr/bin/env python3

import argparse
import json
import logging
import requests
import sys
import urllib
from HTTPAuthOptions import KeycloakTokenAuth
from datetime import datetime, timezone


logging.basicConfig(level=logging.DEBUG)

def parse_args():
    parser = argparse.ArgumentParser(
        description="GSS last product date per type monitoring"
    )
    parser.add_argument(
        "-q", "--query",
        default="$orderby=PublicationDate desc&$top=1",
        help="OData query string (without leading '?'). (default: %(default)s)"
    )
    parser.add_argument(
        "-c", "--config",
        default='latency/config.json',
        help="Path to JSON config file (default: %(default)s)",
    )
    parser.add_argument(
        "-n", "--netrc",
        help="Path to netrc file (default: %(default)s)"
    )
    return parser.parse_args()


# Parse ISO 8601 UTC timestamp like "2025-06-06T08:12:16.630Z"
def parse_iso_date(date_str):
    return datetime.fromisoformat(date_str.replace("Z", "+00:00"))


def check_latency(auth, odata_url, query_params):
    # Send request
    response = requests.get(
        url=odata_url + "/Products",
        params=urllib.parse.urlencode(query_params, quote_via=urllib.parse.quote),
        headers={"Accept": "application/json"},
        auth=auth
    )
    response.raise_for_status()
    data = response.json()

    # Extract and parse PublicationDate
    last_pub_date_str = data["value"][0]["PublicationDate"]
    last_pub_date = parse_iso_date(last_pub_date_str)

    # Compute latency in hours
    now = datetime.now(timezone.utc)
    latency_seconds = int((now - last_pub_date).total_seconds())
    latency_hours = latency_seconds // 3600

    return latency_hours

def prepare_query(custom_query, product_type):
    query = custom_query.split('&')
    params = {}
    for param in query:
        k, v = param.split('=')
        params[k] = v
    if "$filter" in params:
        params["$filter"] = params["$filter"] + f" and startswith(Name,'{product_type}')"
    else:
        params["$filter"] = f"startswith(Name,'{product_type}')"
    return params


if __name__ == '__main__':
    args = parse_args()

    with open(args.config) as file:
        config = json.load(file)

    # Configuration
    config_local = config['local']
    odata_url = config_local['serviceRootUrl']
    query = args.query
    
    if args.netrc:
        netrc_file = args.netrc
    else:
        netrc_file = config.get("netrcFile")

    auth_local = KeycloakTokenAuth(
        server_url=config_local['auth']['tokenEndpoint'],
        realm=config_local['auth']['realm'],
        client_id=config_local['auth']['clientId'],
        netrc_file=netrc_file,
    )

    statuscode = 0
    status_message = []
    latency_message = []
    for product_type in config['productTypes']:
        try:
            query_params = prepare_query(query, product_type)
            latency_hours = check_latency(auth_local, odata_url, query_params)
            if latency_hours is not None and latency_hours <= 72:
                status_message.append(f"OK {product_type}: [{latency_hours}h]")
                latency_message.append(f"{product_type}={latency_hours}")
            else:
                status_message.append(f"WARNING {product_type}: [{latency_hours}h]")
                latency_message.append(f"{product_type}={latency_hours if latency_hours else -1}")
                statuscode = 1 if statuscode < 1 else statuscode
        except Exception as e:
            status_message.append(f"UNKNOWN {product_type}: Error {e}")
            latency_message.append(f"{product_type}=-1")
            statuscode = 3

    print(f"{', '.join(status_message)} | {' '.join(latency_message)}")
    sys.exit(statuscode)
