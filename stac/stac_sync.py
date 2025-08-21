import os
import json
import sys
import datetime
import requests
import logging
from datetime import timezone
from netrc import netrc
from requests.adapters import HTTPAdapter, Retry

"""
Fetches STAC metadata from source catalogue and pushes it to destination catalogue.
Creates missing collections under same name. Uses UUID-like identifier as ID and sets feature title as name.
!!! MAKE SURE TO SET .netrc FILE ENTRIES FOR keycloak and source catalogue !!!
"""

logger = logging.getLogger(__name__)

CONFIG_FILE = '/etc/stac/stac_sync_config.json'
DEFAULT_CONFIG = {
    'last_sync': '2017-01-01T00:00:00.00Z',
    'source_catalog_url': 'https://collgs.cesnet.cz/stac',
    'dest_catalog_url': 'https://resto-test.cloud.cesnet.cz',
    'source_auth': {
        'token_url': 'https://keycloak.grid.cesnet.cz/realms/collgs/protocol/openid-connect/token',
        'client_id': 'token-exchange',
        'grant_type': 'password'
    },
    'dest_auth': {
        'token_url': 'https://resto-test.cloud.cesnet.cz/auth',
    }
}

TIMENOW = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
SOURCE_TOKEN, SOURCE_SESSION, DESTINATION_TOKEN, DESTINATION_SESSION = None, None, None, None


def load_config():
    """
    Loads config file or creates default one and returns it, if config file does not exist yet.
    """
    if not os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'w') as f:
            json.dump(DEFAULT_CONFIG, f, indent=2)
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)


def save_config(config):
    """
    Updates/creates config file with new last_sync time.
    """
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)


def get_keycloak_token(auth_config):
    """
    Get Keycloak token for BA credentials from netrc.
    """
    try:
        username, _, password = check_netrc_entry(auth_config['token_url'])

        data = {
            'grant_type': auth_config['grant_type'],
            'client_id': auth_config['client_id'],
            'username': username,
            'password': password
        }
        response = requests.post(auth_config['token_url'], data=data)
        response.raise_for_status()
        return response.json()['access_token']
    except Exception as e:
        logger.error(f"Failed to get keycloak token: {str(e)}")
        raise


def get_resto_token(auth_config):
    """
    Get resto token for BA credentials from netrc.
    """
    try:
        username, _, password = check_netrc_entry(auth_config['token_url'])

        response = requests.get(auth_config['token_url'])
        response.raise_for_status()
        return response.json()['token']
    except Exception as e:
        logger.error(f"Failed to get resto token: {str(e)}")
        raise


def check_netrc_entry(url):
    """
    Checks entry in ~/.netrc for host. Throws exception if missing or file is unaccessible.
    """
    host = requests.utils.urlparse(url).netloc
    authenticator = netrc()
    netrc_entry = authenticator.hosts.get(host)
    if not netrc_entry:
        raise Exception(f"No auth info for host {host} in .netrc")
    return authenticator.authenticators(host)


def fetch_new_features(config, collections, sync_from, sync_to):
    """
    Fetch features from source catalogue month by month, starting from last syncing time if known till now.
    Returns map of {collection: [features]}
    """
    fetched_features = {}
    count = 0
    for collection in collections:
        params = {
            'datetime': f'{sync_from}/{sync_to}',
            'sortby': 'properties.published',
            'limit': 100
        }

        features = []
        search_url = f"{config['source_catalog_url']}/collections/{collection}/items"
        while True:
            response = SOURCE_SESSION.get(search_url, params=params)
            response.raise_for_status()
            page_features = response.json().get('features', [])
            features.extend(page_features)
            next_link = next((link['href'] for link in response.json().get('links', []) if link.get('rel') == 'next'), None)
            if not next_link:
                break
            search_url = next_link
            params = {}

            if not fetched_features.get(collection):
                fetched_features[collection] = []
            fetched_features[collection].extend(features)
            count += len(features)
    return fetched_features, count


def process_and_push_features(config, features):
    """
    With a map of {collection: [features]}, splits features into sets of 10 features, obtains complete STAC metadata
    and pushes them to destination catalogue.
    """
    if len(features) > 0:
        logger.info(f"Pushing features to destination catalogue")

    for collection, features in features.items():
        if features:
            logger.info(f"Pushing features of {collection} collection")
        for i in range(0, len(features), 10):
            batch = features[i:i + 10]
            self_links = get_self_links(batch)

            complete_features_batch = fetch_complete_features(self_links)
            for c in complete_features_batch:
                c.pop('links')
                c['properties']['name'] = c['id']  # id must be valid UUID
                c['id'] = c['properties']['id']

            push_features(config, collection, complete_features_batch)


def push_features(config, collection, features):
    """
    Pushes bulk of features to destination catalogue.
    """
    dst_features_url = f'{config["dest_catalog_url"]}/collections/{collection}/items'
    response = DESTINATION_SESSION.post(dst_features_url, json={"type": "FeatureCollection", 'features': features})
    if not response.ok:
        if response.status_code == 409:
            # this seems not to be the case in bulk processing
            logger.debug(f"Some features already exists in destination catalogue, skipping.")
        else:
            response.raise_for_status()
    else:
        logger.info(f"Pushed features to {collection}: {[f['properties']['name'] for f in features]}")


def get_month_range(date_str):
    """
    GSS Stac catalogue fails to return too many features (even) in paginated results, so fetch by month.
    """
    date = datetime.datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%fZ')
    next_month = date.replace(month=date.month % 12 + 1, year=date.year + (date.month + 1 > 12))
    return next_month.strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def sync_collections(config):
    """
    Compare existing collections in source catalogue and destination catalogue.
    Returns the list of all collections.
    """
    src_collections_url = f'{config["source_catalog_url"]}/collections'
    response = SOURCE_SESSION.get(src_collections_url)
    response.raise_for_status()
    source_collections = response.json()['collections']

    dst_collections_url = f'{config["dest_catalog_url"]}/collections'
    response = DESTINATION_SESSION.get(dst_collections_url)
    response.raise_for_status()
    target_collections = response.json()['collections']
    target_collection_ids = [collection['id'] for collection in target_collections]


    for collection in source_collections:
        if collection['id'] not in target_collection_ids:
            collection.pop('links')
            logger.info(f"Creating collection {collection['id']}")
            response = DESTINATION_SESSION.post(dst_collections_url, json=collection)
            response.raise_for_status()

    return [collection['id'] for collection in source_collections]


def get_self_links(features):
    """
    Get self links from features (additional asset links are hidden there)
    """
    self_links = []
    for feature in features:
        self_link = next((link['href'] for link in feature.get('links', []) if link.get('rel') == 'self'))
        self_links.append(self_link)
    return self_links


def fetch_complete_features(self_links):
    """
    From self links, fetch complete features (with additional asset links)
    """
    complete_features = []
    for link in self_links:
        response = SOURCE_SESSION.get(link)
        response.raise_for_status()
        complete_features.append(response.json())
    return complete_features


def init_sessions(config):
    """
    Obtains tokens, creates sessions and sets retry handlers.
    """
    global SOURCE_TOKEN, DESTINATION_TOKEN, DESTINATION_SESSION, SOURCE_SESSION
    DESTINATION_TOKEN = get_resto_token(config['dest_auth'])
    SOURCE_TOKEN = get_keycloak_token(config['source_auth'])
    DESTINATION_SESSION = requests.Session()
    DESTINATION_SESSION.headers.update({'Authorization': f'Bearer {DESTINATION_TOKEN}'})
    SOURCE_SESSION = requests.Session()
    SOURCE_SESSION.headers.update({'Authorization': f'Bearer {SOURCE_TOKEN}'})

    retries = Retry(total=5,
                    backoff_factor=0.1,
                    status_forcelist=[500, 502, 503, 504])

    SOURCE_SESSION.mount(config['source_catalog_url'], HTTPAdapter(max_retries=retries))
    DESTINATION_SESSION.mount(config['dest_catalog_url'], HTTPAdapter(max_retries=retries))


def set_logger():
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    if log_level not in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
        log_level = 'INFO'
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stdout,
        level=logging.getLevelName(log_level)
    )


def main():
    set_logger()
    config = load_config()
    logger.info(f"Starting sync with config: {config}")

    init_sessions(config)

    collections = sync_collections(config)

    last_sync = config['last_sync']
    sync_till = get_month_range(last_sync)  # not to overwhelm the catalogue

    while True:
        if last_sync > TIMENOW:
            break
        try:

            logger.info(f"Fetching new features since {last_sync} till {sync_till}")
            features, count = fetch_new_features(
                config,
                collections,
                last_sync,
                sync_till
            )

            logger.info(f"Fetched {count} new features")

            process_and_push_features(config, features)

            last_sync = sync_till
            config['last_sync'] = sync_till if sync_till < TIMENOW else TIMENOW
            sync_till = get_month_range(last_sync)
            save_config(config)

        except Exception as e:
            logger.error(e, exc_info=True)
            return 1
    return 0


if __name__ == "__main__":
    exit(main())
