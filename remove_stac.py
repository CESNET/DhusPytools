#!/usr/bin/python3
import argparse
import netrc
import os
import re
import uuid
from datetime import datetime
from urllib.parse import urlparse

import requests
import yaml
from requests import Session

import sentinel_stac

CONFIG_FILE = "sentinel_config.yml"
NMSPC = b'\x92\x70\x80\x59\x20\x77\x45\xa3\xa4\xf3\x1e\xb4\x28\x78\x9c\xff'
ERR_PREFIX = ""
SUCC_PREFIX = ""
PRODUCT_ID = ""
COLLECTION = ""


def parse_arguments():
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser(
        description='Remove entry from a stac catalogue by product ID.'
                    'The product ID needs to include a prefix, if used to create the stac entry.'
                    'The STAC feature id is computed from the provided product title.'
                    'Example:'
                    './remove_stac.py -i dhr1S3B_SY_2_VG1____20240701T000000_20240701T235959_20240702T122854_EUROPE____________PS2_O_ST_002')
    parser.add_argument('-i',
                        '--productId',
                        required=True,
                        help='Title of product to remove')
    parser.add_argument('-t',
                        '--stacHost',
                        required=False,
                        help='URL of server to push data to, for example https://stac.cesnet.cz.'
                             'Overwrites STAC_HOST configuration option.')

    args = parser.parse_args()
    return args


def die_with_error(msg, detailed_msg="", code=-1):
    """
    Before terminating with exception, writes message to error file.
    Known HTTP error code should be used, otherwise -1 is used.
    """
    rundate = datetime.now().strftime('%Y-%m-%d')
    err_file = ERR_PREFIX + rundate
    create_missing_dir(os.path.dirname(err_file))
    with open(err_file, 'a') as f:
        f.write(f"{PRODUCT_ID},{code}:{msg}\n")
    raise Exception("\n".join([f"{code}: {msg}", detailed_msg]))


def read_configuration():
    """
    Read configuration file.
    """
    with open(CONFIG_FILE, "r") as f:
        return yaml.safe_load(f)


def create_missing_dir(dir_path):
    """
    Creates directory, if it does not exist yet (including all missing directories in the path).
    """
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)


def get_stac_id(product_title):
    """
    Convert product name to STAC feature id. The defined namespace is required.
    """
    namespace = uuid.UUID(bytes=NMSPC)
    generated_uuid = uuid.uuid5(namespace, product_title)
    return generated_uuid


def check_host(stac_host):
    """Checks, if netrc entry is stored for the STAC host."""
    if not stac_host:
        die_with_error("STAC host not configured properly!")

    try:
        auth_info = netrc.netrc()
        if not auth_info.authenticators(urlparse(stac_host).netloc):
            die_with_error(f"Host {urlparse(stac_host)} not found in authentication credentials in the .netrc file!")
    except (FileNotFoundError, netrc.NetrcParseError) as e:
        die_with_error(f"Error parsing authentication file .netrc in the home directory.")


def get_auth_token(token_url):
    """
    Gets token for communication with API from token url.
    """
    response = requests.get(token_url)
    if not response.ok:
        die_with_error(f"Could not obtain API token from {token_url}", response.text, response.status_code)
    return response.json()["token"]


def get_auth_session(token):
    """
    Creates session which overwrites the BA credentials set in the ~/.netrc file by auth token.
    """
    token_session = Session()
    token_session.trust_env = False  # need to overwrite the authorization header, otherwise BA is used
    token_session.headers.update({"Authorization": f"Bearer {token}"})
    return token_session


def map_to_collection(product_name):
    """
    Returns the normalized collection name for a given product.
    """
    for pattern, collection in sentinel_stac.product_collection_mapping.items():
        if re.match(pattern, product_name):
            return collection
    die_with_error("Could not match product to collection name! Probably missing in the sentinel_stac.py mappings.")



def remove_from_catalogue(stac_host, feature_id):
    """
    Removes single entry from a STAC catalogue. Obtains token first.
    """
    url = f"{stac_host}/collections/{COLLECTION}/items/{feature_id}"
    print(f"Removing STAC entry {feature_id} from {url}")

    token = get_auth_token(f"{stac_host}/auth")
    token_session = get_auth_session(token)
    response = token_session.delete(url)

    rundate = datetime.now().strftime('%Y-%m-%d')

    if response.ok:
        succ_file = SUCC_PREFIX + rundate
        create_missing_dir(os.path.dirname(succ_file))
        with open(succ_file, 'a') as f:
            f.write(f"{COLLECTION},{PRODUCT_ID}\n")
    elif response.status_code == 404:
        die_with_error(f"Wrong URL, or feature {feature_id} under collection {COLLECTION} not found!")
    elif response.status_code == 403:
        die_with_error(f"Insufficient permissions to remove feature {feature_id}!")
    else:
        die_with_error(f"Request to upload STAC file failed", response.text, response.status_code)

def main():
    args = parse_arguments()
    config = read_configuration()
    global PRODUCT_ID, COLLECTION, SUCC_PREFIX, ERR_PREFIX
    SUCC_PREFIX = config.get("SUCC_PREFIX_REMOVAL")
    ERR_PREFIX = config.get("ERR_PREFIX_REMOVAL")
    PRODUCT_ID = args.productId
    prefix = config.get("SALT")
    unsalted_title = PRODUCT_ID.split(prefix)[1] if prefix and prefix in PRODUCT_ID else PRODUCT_ID
    COLLECTION = map_to_collection(unsalted_title)

    stac_host = args.stacHost or config.get("STAC_HOST")
    check_host(stac_host)

    feature_id = get_stac_id(PRODUCT_ID)

    remove_from_catalogue(stac_host, feature_id)

if __name__ == "__main__":
    main()