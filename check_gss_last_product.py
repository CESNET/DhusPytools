#!/usr/bin/env python3

import requests
import sys
from datetime import datetime, timezone
import logging
logging.basicConfig(level=logging.DEBUG)
from HTTPAuthOptions import KeycloakTokenAuth
import argparse


def parse_args():
    parser = argparse.ArgumentParser(
        description="Configure OData and Keycloak endpoints the check gss latency."
    )
    parser.add_argument(
        "-u", "--odata-url",
        #default="https://collgs.cesnet.cz/odata/v1/Products"
        default="https://fe1.dhr.cesnet.cz/odata/v1/Products",
        help="Base OData endpoint URL."
    )
    parser.add_argument(
        "-q", "--query",
        default="?$orderby=PublicationDate desc&$top=1",
        help="OData query string (include leading '?')."
    )
    parser.add_argument(
        "-t", "--token-url",
        default="https://keycloak.grid.cesnet.cz",
        help="Keycloak base URL."
    )
    parser.add_argument(
        "-r", "--realm",
        #default="collgs"
        default="dhr",
        help="Keycloak realm."
    )
    parser.add_argument(
        "-c", "--client-id",
        default="gss",
        help="Keycloak client ID."
    )
    parser.add_argument(
        "-n", "--netrc-file",
        default=None,
        help="Path to custom netrc file."
    )
    return parser.parse_args()

# Parse ISO 8601 UTC timestamp like "2025-06-06T08:12:16.630Z"
def parse_iso_date(date_str):
    return datetime.fromisoformat(date_str.replace("Z", "+00:00"))


if __name__ == '__main__':
    args = parse_args()

    # Configuration
    ODATA_URL = args.odata_url
    QUERY = args.query

    # Keycloak credentials
    TOKEN_URL = args.token_url
    REALM = args.realm
    CLIENT_ID = args.client_id
    NETRC_FILE = args.netrc_file
    
    # Auth using KeycloakTokenAuth
    AUTH = KeycloakTokenAuth(
        server_url=TOKEN_URL,
        realm=REALM,
        client_id=CLIENT_ID,
        netrc_file=NETRC_FILE,
    )

    try:
        # Build full URL with query string
        full_url = ODATA_URL + QUERY

        # Send request
        response = requests.get(
            full_url,
            headers={"Accept": "application/json"},
            auth=AUTH
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

        # Nagios-compatible output
        if latency_hours <= 72:
            print(f"OK - Latest publication: {latency_hours}[h] ago | latency={latency_hours}")
            sys.exit(0)
        elif latency_hours > 72:
            print(f"WARNING - Latest publication from {last_pub_date.isoformat()} | latency={latency_hours}")
            sys.exit(1)
        else:
            print("UNKNOWN")
            sys.exit(3)

    except Exception as e:
        print(f"UNKNOWN - Error: {e}")
        sys.exit(3)
