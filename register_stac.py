#!/usr/bin/python3

import argparse
import netrc
import os
import re
import sys
import tempfile
from datetime import datetime
from urllib.parse import urlparse

import defusedxml.ElementTree
import pystac
import requests
import stactools.sentinel1.grd.stac
import stactools.sentinel1.slc.stac
import stactools.sentinel2.stac
import stactools.sentinel3.stac
from stactools.sentinel3 import constants
import stactools.sentinel5p.stac
import yaml
from requests import Session
from stactools.sentinel3.file_extension_updated import FileExtensionUpdated
from tqdm import tqdm

import sentinel_stac

CONFIG_FILE = "sentinel_config.yml"
ERR_PREFIX = ""
SUCC_PREFIX = ""
PRODUCT_ID = None
COLLECTION = None

# Stactools fixes
# Our S3 data don't contain reducedMeasurementData
stactools.sentinel3.constants.SRAL_L2_LAN_WAT_KEYS.remove("reducedMeasurementData")

# Monkey-patch class method of Sentinel3 module to avoid casting error
def new_ext(cls, obj: pystac.Asset, add_if_missing: bool = False):
    return super(FileExtensionUpdated, cls).ext(obj, add_if_missing)
FileExtensionUpdated.ext = classmethod(new_ext)


def parse_arguments():
    """
    Parse command line arguments. Check if combinations are valid.
    """
    parser = argparse.ArgumentParser(
        description='Generate Sentinel 1, 2, 3 and 5P metadata in STAC format from data fetched from a Sentinel OData '
                    'API. Configuration needs to be stored in sentinel_config.yml file and can be partly overwritten '
                    'by arguments. See README. The program requires --productId (-i) argument to be '
                    'provided and --save (-s) and/or --push (-p) option to be used. Example usage: '
                    './register_stac.py -p -i 72250006-4290-40ec-987d-3ed771e690f3')
    parser.add_argument('-i',
                        '--productId',
                        required=True,
                        help='UUID of product to generate STAC data for.')
    parser.add_argument('-e',
                        '--sentinelHost',
                        required=False,
                        help='URL of server to fetch Sentinel data from, for example https://dhr1.cesnet.cz/.'
                             'Overwrites SENTINEL_HOST configuration option.')
    parser.add_argument('-t',
                        '--stacHost',
                        required=False,
                        help='URL of server to push data to, for example https://stac.cesnet.cz.'
                             'Overwrites STAC_HOST configuration option.')
    parser.add_argument('-l',
                        '--localDir',
                        required=False,
                        help='Local folder to which STAC json files shall be stored if --save option specified. '
                             'Overwrites LOCAL_DIR configuration option.')
    parser.add_argument('-p',
                        '--push',
                        required=False,
                        action='store_true',
                        help='Enables pushing data to the catalogue at --stacHost.')
    parser.add_argument('-s',
                        '--save',
                        required=False,
                        action='store_true',
                        help='Enables saving data locally.')
    parser.add_argument('-o',
                        '--overwrite',
                        action='store_true',
                        required=False,
                        help='Include this flag to overwrite existing entries in the STAC catalogue.')

    args = parser.parse_args()
    if not args.push and not args.save:
        die_with_error('--push or --save required to take any action')
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
        f.write(f"{COLLECTION},{PRODUCT_ID},{code}:{msg}\n")
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


def request_with_progress(url, output_path):
    """
    Downloads a file from a URL and saves it to the specified output path, with a progress bar.
    """
    # if ~/.netrc file is found, it is used automatically as a basic auth for all requests
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))  # Total size in bytes
    block_size = 1024  # Size of each block (1 KB)

    if not response.ok:
        die_with_error(f"Request to fetch file {url} failed.", response.text, response.status_code)

    progress_bar = tqdm(total=total_size,
                        unit='iB',
                        unit_scale=True,
                        desc=f"Fetching file {output_path.split('/')[-1]}",
                        leave=True,
                        file=sys.stdout)

    with open(output_path, "wb") as f:
        for data in response.iter_content(block_size):
            progress_bar.update(len(data))
            f.write(data)

    progress_bar.close()


def fetch_product_data(sentinel_host, metadata_dir):
    """
    Fetch Sentinel data for given product UUID from the specified host.
    """
    url = f"{sentinel_host}/odata/v1/Products('{PRODUCT_ID}')/Nodes"
    output_path = os.path.join(metadata_dir, "node.xml")
    request_with_progress(url, output_path)
    with open(output_path, "rb") as f:
        metadata = f.read()
        data = defusedxml.ElementTree.fromstring(metadata)
    namespaces = {'atom': 'http://www.w3.org/2005/Atom'}

    entry_node = data.find('atom:entry', namespaces)
    title_node = entry_node.find('atom:title', namespaces) if entry_node is not None else None
    title = title_node.text if title_node is not None else None
    product_node = entry_node.find('atom:id', namespaces) if entry_node is not None else None
    product_url = product_node.text if product_node is not None else None
    platform = title[0:2] if title else None
    global COLLECTION
    COLLECTION = map_to_collection(title)

    if not title or not product_url:
        die_with_error("Missing required title or product url for product.")

    print(f"Parsed product data for product (UUID {PRODUCT_ID}):\n"
          f"*  Title ID: {title}\n"
          f"*  Platform: {platform}\n"
          f"*  Collection: {COLLECTION}\n"
          f"*  Product URL: {product_url}")
    return title, product_url, platform


def check_hosts(sentinel_host, stac_host, push):
    """
    Checks sentinel_host and stac_host variables were resolved and .netrc file contains authentication credentials.
    """
    if not sentinel_host:
        die_with_error("Sentinel host not configured properly!")
    if not stac_host and push:
        die_with_error("STAC host not configured properly!")

    try:
        auth_info = netrc.netrc()
        if not auth_info.authenticators(urlparse(sentinel_host).netloc):
            die_with_error(
                f"Host {urlparse(sentinel_host)} not found in authentication credentials in the .netrc file!")
        if push and not auth_info.authenticators(urlparse(stac_host).netloc):
            die_with_error(f"Host {urlparse(stac_host)} not found in authentication credentials in the .netrc file!")
    except (FileNotFoundError, netrc.NetrcParseError) as e:
        die_with_error(f"Error parsing authentication file .netrc in the home directory.")


def map_to_collection(product_name):
    """
    Returns the normalized collection name for a given product.
    """
    for pattern, collection in sentinel_stac.product_collection_mapping.items():
        if re.match(pattern, product_name):
            return collection
    die_with_error("Could not match product to collection name! Probably missing in the sentinel_stac.py mappings.")


def fetch_platform_metadata(product_url, metadata_dir, platform):
    """
    Fetches metadata from product's /Nodes data and stores them in the metadata directory.
    """
    if platform.lower() == "s1":
        platform_files = sentinel_stac.s1_files
    elif platform.lower() == "s2":
        platform_files = sentinel_stac.s2_files
    elif platform.lower() == "s3":
        platform_files = sentinel_stac.s3_files
    elif platform.lower() == "s5":
        platform_files = sentinel_stac.s5_files
    else:
        die_with_error(f"Platform {platform} not supported!")
    for file in platform_files:
        source_url = f"{product_url}/Nodes('{file}')/$value"
        output_file = os.path.join(metadata_dir, file)
        request_with_progress(source_url, output_file)


def fetch_nested_s1_files(metadata, product_url, metadata_dir):
    """
    From the processed metadata file downloads the missing metadata files, which we know
    the stactools will be working with.
    """
    filepaths = metadata.annotation_hrefs + metadata.noise_hrefs + metadata.calibration_hrefs
    for ref_name, filepath in filepaths:
        url_path_extension = filepath.split(f"{metadata_dir}{'/'}")[1]
        url_path_segments = url_path_extension.split('/')
        nested_file_url = product_url + ''.join(f"/Nodes('{segment}')" for segment in url_path_segments) + "/$value"
        create_missing_dir(os.path.dirname(filepath))
        request_with_progress(nested_file_url, filepath)


def fetch_nested_s2_files(metadata, product_url, metadata_dir):
    """
    From the processed metadata file downloads the missing metadata files, which we know
    the stactools will be working with.
    """
    filepaths = [metadata.product_metadata_href,
                 metadata.granule_metadata_href,
                 metadata.inspire_metadata_href,
                 metadata.datastrip_metadata_href,
                 ]
    for filepath in filepaths:
        url_path_extension = filepath.split(f"{metadata_dir}{'/'}")[1]
        url_path_segments = url_path_extension.split('/')
        nested_file_url = product_url + ''.join(f"/Nodes('{segment}')" for segment in url_path_segments) + "/$value"
        create_missing_dir(os.path.dirname(filepath))
        request_with_progress(nested_file_url, filepath)


def fetch_s5_metadata(product_url, title, metadata_dir):
    """
    Fetches metadata directly from the product node - {{hostname}}/odata/v1/Products('{{UUID}}')/Node({{'title'}}/$value
    and stores them in a file named by the product title in the metadata directory.
    """
    url = f"{product_url}/$value"
    output_file = os.path.join(metadata_dir, title)
    request_with_progress(url, output_file)


def regenerate_href_links(stacfile_path, metadata_dir, product_url):
    """
    Replaces href links in the final json containing the local path to contain the OData path and format.
    Cannot use stactools' create_item function parameters for this change, as the stac module is then actually reading
    from the hrefs, or the change does not affect all the hrefs.
    """
    print("Regenerating href links")
    new_file = stacfile_path.split('/')
    new_file[-1] = 'new_' + new_file[-1]
    new_file = os.path.join('/', *new_file)
    with (open(stacfile_path, 'r') as infile, open(new_file, 'w') as outfile):
        for line in infile:
            if metadata_dir in line:
                split_line = line.split('"')  # [' ', 'href', ': ', 'matadata_dir/resource/path', '\n']
                url_path_segments = split_line[-2].split(f"{metadata_dir}{'/'}")[1].split("/")
                correct_link = product_url + ''.join(
                    f"/Nodes('{segment}')" for segment in url_path_segments) + "/$value"
                split_line[-2] = correct_link
                outfile.write('"'.join(split_line))
            else:
                outfile.write(line)
    os.replace(new_file, stacfile_path)


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


def update_catalogue_entry(stac_host, entry_id, json_data, auth_token=None):
    """
    Updates stac entry by fully rewriting it
    """
    url = f"{stac_host}/collections/{COLLECTION}/items/{entry_id}"
    print(f"Overwriting existing product entry in STAC catalogue.")

    token = auth_token or get_auth_token(f"{stac_host}/auth")
    token_session = get_auth_session(token)

    response = token_session.put(url, data=json_data)
    if not response.ok:
        die_with_error(f"Could not remove existing product from catalogue.", response.text, response.status_code)


def upload_to_catalogue(stac_host, stac_filepath, overwrite=False):
    """
    Uploads the stac file to the catalogue.
    Reports progress in the preconfigured files suffixed by the current date.
    """
    url = f"{stac_host}/collections/{COLLECTION}/items"
    print(f"Uploading STAC data to {url}")

    token = get_auth_token(f"{stac_host}/auth")

    with open(stac_filepath, 'r') as file:
        json_data = file.read()
        rundate = datetime.now().strftime('%Y-%m-%d')
        token_session = get_auth_session(token)
        response = token_session.post(url, data=json_data)

        if response.ok:
            succ_file = SUCC_PREFIX + rundate
            create_missing_dir(os.path.dirname(succ_file))
            with open(succ_file, 'a') as f:
                f.write(f"{COLLECTION},{PRODUCT_ID}\n")
        elif response.status_code == 409:
            if not overwrite:
                # don't die
                err_file = ERR_PREFIX + rundate
                create_missing_dir(os.path.dirname(err_file))
                with open(err_file, 'a') as f:
                    f.write(f"{COLLECTION},{PRODUCT_ID},0,Skipped existing product\n")
                print("Product already registered, skipping.")
            else:
                if response.text and "Feature" in response.text and "ErrorMessage" in response.text:
                    stac_product_id = response.json().get("ErrorMessage").split(" ")[1]
                    update_catalogue_entry(stac_host, COLLECTION, stac_product_id, json_data, token)
                else:
                    die_with_error("Cannot update existing entry, feature id expected in response not found.")
        elif response.status_code == 404:
            die_with_error("Wrong URL, or collection does not exist.", response.text, response.status_code)
        else:
            die_with_error(f"Request to upload STAC file failed", response.text, response.status_code)


def main():
    args = parse_arguments()
    config = read_configuration()
    global PRODUCT_ID
    PRODUCT_ID = args.productId

    sentinel_host = args.sentinelHost or config.get("SENTINEL_HOST")
    stac_host = args.stacHost or config.get("STAC_HOST")

    if args.save and config.get("LOCAL_DIR") is None and args.localDir is None:
        die_with_error("Flag --save was provided, but LOCAL_DIR option not configured and not specified "
                       "in the --localDir argument!")

    stac_storage = args.localDir or os.path.join(config.get("LOCAL_DIR"), "register_stac")
    if stac_storage is not None:
        if not os.path.isabs(stac_storage):
            die_with_error("Valid path not used for the stac storage argument - expected an absolute directory path!")
        create_missing_dir(os.path.dirname(stac_storage))

    global SUCC_PREFIX, ERR_PREFIX
    SUCC_PREFIX = config.get("SUCC_PREFIX")
    ERR_PREFIX = config.get("ERR_PREFIX")
    if args.push and (SUCC_PREFIX is None or ERR_PREFIX is None):
        die_with_error("Flag --push was provided, but SUCC_PREFIX and ERR_PREFIX need to be set in the configuration "
                       "file for logging!")

    if args.push and not stac_host:
        die_with_error('--push requires --stacHost argument or STAC_HOST configuration option to be set!')

    check_hosts(sentinel_host, stac_host, args.push)

    with (tempfile.TemporaryDirectory() as metadata_dir):
        print(f"Created temporary directory: {metadata_dir}")

        title, product_url, platform = fetch_product_data(sentinel_host, metadata_dir)

        metadata_dir = os.path.join(metadata_dir, title)
        os.mkdir(metadata_dir)

        fetch_platform_metadata(product_url, metadata_dir, platform)

        try:
            if platform.lower() == "s1":
                product_type = title.split("_")[2]
                if product_type.lower() == "slc":
                    metadata = stactools.sentinel1.slc.stac.SLCMetadataLinks(metadata_dir)
                    fetch_nested_s1_files(metadata, product_url, metadata_dir)
                    item = stactools.sentinel1.slc.stac.create_item(granule_href=metadata_dir)
                else:
                    metadata = stactools.sentinel1.grd.stac.MetadataLinks(metadata_dir)
                    fetch_nested_s1_files(metadata, product_url, metadata_dir)
                    item = stactools.sentinel1.grd.stac.create_item(granule_href=metadata_dir)
            elif platform.lower() == "s2":
                safe_manifest = stactools.sentinel2.stac.SafeManifest(metadata_dir)
                fetch_nested_s2_files(safe_manifest, product_url, metadata_dir)
                item = stactools.sentinel2.stac.create_item(granule_href=metadata_dir)
            elif platform.lower() == "s3":
                item = stactools.sentinel3.stac.create_item(granule_href=metadata_dir, skip_nc=True)
            elif platform.lower() == "s5":
                fetch_s5_metadata(product_url, title, metadata_dir)
                item = stactools.sentinel5p.stac.create_item(os.path.join(metadata_dir, title))
            else:
                raise Exception(f"Unknown platform {platform}")
        except Exception as e:
            die_with_error(e.args[0] if e.args and len(str(e.args[0])) > 5 else str(e))

        stac_storage = stac_storage if args.save else metadata_dir
        stac_filepath = os.path.join(stac_storage, "{}.json".format(item.id))

        print(f"Writing metadata to file: {stac_filepath}")
        item.save_object(dest_href=stac_filepath, include_self_link=False)

        regenerate_href_links(stac_filepath, metadata_dir, product_url)

        if args.push:
            upload_to_catalogue(stac_host, stac_filepath, overwrite=args.overwrite)
        print("Finished")


if __name__ == "__main__":
    main()
