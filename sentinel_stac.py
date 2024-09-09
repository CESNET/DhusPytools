import netrc
import os
import re
from urllib.parse import urlparse

import requests
from datetime import datetime
from requests import Session

import yaml


# imported constants, should be updated by developers
CONFIG_FILE = "sentinel_config.yml"
S1_FILES = ["manifest.safe"]
S2_FILES = ["manifest.safe"]
S3_FILES = ["xfdumanifest.xml"]
S5_FILES = []
PRODUCT_COLLECTION_MAPPING = {
    r'^S1[A-DP]_.._GRD[HM]_.*': 'sentinel-1-grd',
    r'^S1[A-DP]_.._SLC__.*': 'sentinel-1-slc',
    r'^S1[A-DP]_.._RAW__.*': 'sentinel-1-raw',
    r'^S1[A-DP]_.._OCN__.*': 'sentinel-1-ocn',
    r'^S2[A-DP]_MSIL1B_.*': 'sentinel-2-l1b',
    r'^S2[A-DP]_MSIL1C_.*': 'sentinel-2-l1c',
    r'^S2[A-DP]_MSIL2A_.*': 'sentinel-2-l2a',
    r'^S3[A-DP]_OL_1_.*': 'sentinel-3-olci-l1b',
    r'^S3[A-DP]_OL_2_.*': 'sentinel-3-olci-l2',
    r'^S3[A-DP]_SL_1_.*': 'sentinel-3-slstr-l1b',
    r'^S3[A-DP]_SL_2_.*': 'sentinel-3-slstr-l2',
    r'^S3[A-DP]_SR_1_.*': 'sentinel-3-stm-l1',
    r'^S3[A-DP]_SR_2_.*': 'sentinel-3-stm-l2',
    r'^S3[A-DP]_SY_1_.*': 'sentinel-3-syn-l1',
    r'^S3[A-DP]_SY_2_.*': 'sentinel-3-syn-l2',
    r'^S5[A-DP]_OFFL_L1_.*': 'sentinel-5p-l1',
    r'^S5[A-DP]_NRTI_L1_.*': 'sentinel-5p-l1',
    r'^S5[A-DP]_OFFL_L2_.*': 'sentinel-5p-l2',
    r'^S5[A-DP]_NRTI_L2_.*': 'sentinel-5p-l2',
}

ERR_FILE = ""


# helper functions
def die_with_error(product_id, msg, detailed_msg="", code=-1):
    """
    Before terminating with exception, writes message to error file.
    Known HTTP error code should be used, otherwise -1 is used.
    """
    create_missing_dir(os.path.dirname(ERR_FILE))
    with open(ERR_FILE, 'a') as f:
        f.write(f"{product_id},{code}:{msg}\n")
    raise Exception("\n".join([f"{code}: {msg}", detailed_msg]))


def create_missing_dir(dir_path):
    """
    Creates directory, if it does not exist yet (including all missing directories in the path).
    """
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)


def read_configuration(config_file):
    """
    Read configuration file and store path to error logs file.
    """
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)
        rundate = datetime.now().strftime('%Y-%m-%d')
        global ERR_FILE
        ERR_FILE = config.get("ERR_PREFIX_REMOVAL") + rundate
        return config


def get_auth_session(token):
    """
    Creates session which overwrites the BA credentials set in the ~/.netrc file by auth token.
    """
    token_session = Session()
    token_session.trust_env = False  # need to overwrite the authorization header, otherwise BA is used
    token_session.headers.update({"Authorization": f"Bearer {token}"})
    return token_session


def map_to_collection(product_id):
    """
    Returns the normalized collection name for a given product.
    """
    for pattern, collection in PRODUCT_COLLECTION_MAPPING.items():
        if re.match(pattern, product_id):
            return collection
    die_with_error(product_id, "Could not match product to collection name! Probably missing in the sentinel_stac.py mappings.")



def get_auth_token(token_url, product_id):
    """
    Gets token for communication with API from token url.
    """
    response = requests.get(token_url)
    if not response.ok:
        die_with_error(product_id, f"Could not obtain API token from {token_url}", response.text, response.status_code)
    return response.json()["token"]


def check_host(product_id, host):
    """
    Checks, if netrc entry is stored for the STAC host.
    """
    if not host:
        die_with_error(product_id, "STAC host not configured properly!")

    try:
        auth_info = netrc.netrc()
        if not auth_info.authenticators(urlparse(host).netloc):
            die_with_error(product_id, f"Host {urlparse(host)} not found in authentication credentials in the .netrc file!")
    except (FileNotFoundError, netrc.NetrcParseError) as e:
        die_with_error(product_id, f"Error parsing authentication file .netrc in the home directory.")

