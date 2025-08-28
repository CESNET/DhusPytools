import sys
import logging
logging.basicConfig(level=logging.INFO)

import requests
import HTTPAuthOptions
# token here -> .token
# https://keycloak.grid.cesnet.cz/token-portal/index.php

# Keycloak authentication data
TOKEN_URL="https://keycloak.grid.cesnet.cz/auth"
REALM = "dhr"
CLIENT_ID="gss"

MISSIONS = ["S1", "S2A", "S2B", "S2C", "S3", "S5P"]
#MISSIONS = ["S2A", "S2C"]
SITE_URLS_AUTH = {
    #"https://dhr1.cesnet.cz/odata/v2": None,
    "https://collgs.cesnet.cz/odata/v1": HTTPAuthOptions.HTTPBearerAuth(),
    #"https://gss.vm.cesnet.cz/odata/v1": HTTPAuthOptions.KeycloakTokenAuth(server_url=TOKEN_URL, realm=REALM, client_id=CLIENT_ID),
}

def get_mission_product_count(site, mission, year):
    url = f"{site}/Products/$count?$filter=startswith(Name,'{mission}')" 
    if year:
        url += f" and ContentDate/Start lt {year}-01-01T00:00:00.000Z and Online eq True"
    r = requests.get(url, auth=SITE_URLS_AUTH.get(site))
    if r.status_code == 200:
        return r.text
    else:
        print(r.text)
        return "FAIL"

if __name__ == "__main__":
    
    if len(sys.argv) > 1:
        year = sys.argv[1]
    else:
        year = None
    print(f"Year lt {year}")

    for site in SITE_URLS_AUTH:
        for mission in MISSIONS:
            print(f"Site: {site} Mission: {mission} Count: {get_mission_product_count(site, mission, year)}")
