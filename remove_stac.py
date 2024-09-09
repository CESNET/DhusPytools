#!/usr/bin/python3
import argparse
import uuid

from sentinel_stac import *

NMSPC = b'\x92\x70\x80\x59\x20\x77\x45\xa3\xa4\xf3\x1e\xb4\x28\x78\x9c\xff'
SUCC_PREFIX = ""
PRODUCT_NAME = ""
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
                    './remove_stac.py -p dhr1S3B_SY_2_VG1____20240701T000000_20240701T235959_20240702T122854_EUROPE____________PS2_O_ST_002')
    parser.add_argument('-p',
                        '--productName',
                        required=True,
                        help='Title of product to remove')
    parser.add_argument('-t',
                        '--stacHost',
                        required=False,
                        help='URL of server to push data to, for example https://stac.cesnet.cz.'
                             'Overwrites STAC_HOST configuration option.')

    args = parser.parse_args()
    return args


def get_stac_id(product_title):
    """
    Convert product name to STAC feature id. The defined namespace is required.
    """
    namespace = uuid.UUID(bytes=NMSPC)
    generated_uuid = uuid.uuid5(namespace, product_title)
    return generated_uuid


def remove_from_catalogue(stac_host, feature_id):
    """
    Removes single entry from a STAC catalogue. Obtains token first.
    """
    url = f"{stac_host}/collections/{COLLECTION}/items/{feature_id}"
    print(f"Removing STAC entry {feature_id} from {url}")

    token = get_auth_token(f"{stac_host}/auth", PRODUCT_NAME)
    token_session = get_auth_session(token)
    response = token_session.delete(url)

    rundate = datetime.now().strftime('%Y-%m-%d')

    if response.ok:
        succ_file = SUCC_PREFIX + rundate
        create_missing_dir(os.path.dirname(succ_file))
        with open(succ_file, 'a') as f:
            f.write(f"{COLLECTION},{PRODUCT_NAME}\n")
    elif response.status_code == 404:
        die_with_error(PRODUCT_NAME, f"Wrong URL, or feature {feature_id} under collection {COLLECTION} not found!")
    elif response.status_code == 403:
        die_with_error(PRODUCT_NAME, f"Insufficient permissions to remove feature {feature_id}!")
    else:
        die_with_error(PRODUCT_NAME, f"Request to upload STAC file failed", response.text, response.status_code)

def main():
    args = parse_arguments()
    config = read_configuration(CONFIG_FILE)
    global PRODUCT_NAME, COLLECTION, SUCC_PREFIX
    SUCC_PREFIX = config.get("SUCC_PREFIX_REMOVAL")
    PRODUCT_NAME = args.productName
    prefix = config.get("SALT")
    unsalted_title = PRODUCT_NAME.split(prefix)[1] if prefix and prefix in PRODUCT_NAME else PRODUCT_NAME
    COLLECTION = map_to_collection(unsalted_title)
    stac_host = args.stacHost or config.get("STAC_HOST")
    check_host(PRODUCT_NAME, stac_host)
    feature_id = get_stac_id(PRODUCT_NAME)
    remove_from_catalogue(stac_host, feature_id)

if __name__ == "__main__":
    main()