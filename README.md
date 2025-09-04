# Dependencies
Create python virtualenv and install dependent libraries.
```
virtualenv .
source bin/activate
pip install -r requirements.txt
```

# Generate new list
The **gen_new_list.py** generates a list of new product IDs in the DHuS database added since the last run
of this script and saves them. Otherwise, generates list of IDs from the last 30 days. The list of IDs is then
stored in a file.

### Configuration
**Authentication**: Basic auth is resolved automatically by the Requests library by reading a **~/.netrc** file. Make sure
to set up the correct entry (DHuS URL) there.

**Configuration file**: The **sentinel_config.yml** contains the URL to the Sentinel data hub host.

**Command line arguments**: The command line options supersede the configuration file settings. Run help to list all
configurable parameters: `./gen_new_list.py -h`

# Register Sentinel to STAC catalogue
The **register_stack.py** script fetches Sentinel 1, 2, 3 and 5P metadata from a data hub
and transforms it into a [STAC](https://stacspec.org/en) format. It can publish the results to a STAC catalogue.
The transformation is done by imported [stactools](https://github.com/stac-utils/stactools) modules.

## Automatically update missing products to STAC
**check_new_register_stac.py** calls previous scripts to fetch new products, transform them to STAC metadata and push them to the catalogue.

### Configuration
There are several ways to configure the script's behaviour:

**Preconfigured mappings**: The **stac_collections.py** file contains general constants that should be updated
by developers if necessary. You can modify the options if needed.

**Configuration file**: The **sentinel_config.yml** contains the URLs to the Sentinel data hub host and the STAC catalogue
host, log file prefixes and path to a location to save the data.

**Command line arguments**: The command line options supersede the configuration file settings. Run help to list all
configurable parameters: `./register_stack.py -h`

**Authentication**: Basic auth is resolved automatically by the Requests library by reading a **~/.netrc** file. Make sure
to set up the correct entries (Sentinel and STAC host URL) there.

# GSS user test activity 
Automation of the COPE-SRCO-PL-2400437 GSS user test activity v1.1.
Perform Odata queries: Odata filters, queries by attributes and nodes inspection for some products per each product type in a random way.

## Usage
Example:
```
python user-test-activity3.py -b
```

Use with `-b` for basic auth file `.netrc` which contains `machine`, `login`, `password` records. See man curl.

Use with `-t` for basic auth file `.token` which contains single `token` line.

Use with `-k` for keycloak authentication. Credentials are read from the `.netrc`.

Use `-d` to increase verbosity. Specify multiple times to increase more.

Custom filters could be defined in `filters.txt` file.

# GSS Admin API Client
Usage:
```
gss-admin-client [-h] [--name NAME] [--data-file DATA_FILE] [-d] [--pretty]
    {get,create,update,delete,dump}
    [{datastores/hfs,consumers,producers,quotas,metadatastores/solr,datastores/timebased,swiftcredentials,jobs,datastores/swiftgroup,datastores/swift}]
```

To get full parameter list run
```
gss-admin-client --help
```

`.netrc` authentication is used as default.

You are required to change the `BASE_URL` within the script to match your admin-api url.

## Example
Get producers
```
gss-admin-client get producers
gss-admin-client get producers --name producer-name --pretty
```

Update existing entity takes name directly from the json file
```
gss-admin-client update producers --data-file datahub-producer-1.json
```

Dump all entities
```
gss-admin-client dump
```

It is possible to feed output of get/dump back to the api again.

## Basic Auth
To use basic authentication one should change the `auth` line in the script.

