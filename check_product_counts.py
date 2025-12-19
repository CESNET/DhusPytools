import sys
import logging
import json
import requests
import HTTPAuthOptions
# token here -> .token
# https://keycloak.grid.cesnet.cz/token-portal/index.php

# Keycloak authentication data
TOKEN_URL="https://keycloak.grid.cesnet.cz"
REALM = "dhr"
CLIENT_ID="gss"

MISSIONS = ["S1", "S2A", "S2B", "S2C", "S3", "S5P"]
SITE_URL = "https://gss.vm.cesnet.cz/odata/v1"

def get_mission_product_count(site, auth, mission, year):
    url = f"{site}/Products/$count?$filter=startswith(Name,'{mission}')" 
    if year:
        url += f" and ContentDate/Start lt {year}-01-01T00:00:00.000Z and Online eq True"
    r = requests.get(url, auth=auth)
    if r.status_code == 200:
        return r.text
    else:
        logging.debug(r.text)
        return "FAIL"

def gss_metrics(site_url, auth=None, year=None):
    if not auth:
        auth = HTTPAuthOptions.KeycloakTokenAuth(server_url=TOKEN_URL, realm=REALM, client_id=CLIENT_ID)
    
    metrics = []
    
    for mission in MISSIONS:
        mission_product_count = get_mission_product_count(site_url, auth, mission, year)
        logging.debug(f"Site: {site_url} Mission: {mission} Count: {mission_product_count}")
        prom_line = f'gss_product_count{{site="{site_url}", mission="{mission}"}} {mission_product_count}'
        metrics.append(prom_line)
    
    return metrics


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    if len(sys.argv) > 1:
        year = sys.argv[1]
    else:
        year = None
    logging.debug(f"Year lt {year}")
    
    print("\n".join(gss_metrics(SITE_URL, None, year)))
