# Register Sentinel to STAC

The **register_stack.py** script fetches Sentinel 1, 2, 3 and 5P metadata from a data hub
and transforms it into a [STAC](https://stacspec.org/en) format. It can publish the results to a STAC catalogue.
The transformation is done by imported [stactools](https://github.com/stac-utils/stactools) modules.

## Dependencies
Install dependent libraries with `pip install -r requirements.txt`.

## Configuration
There are several ways to configure the script's behaviour:

**Preconfigured mappings**: The **stac_collections.py** file contains general constants that should be updated
by developers if necessary. You can modify the options if needed.

**Configuration file**: The **register_stac.yml** contains the URLs to the Sentinel data hub host and the STAC catalogue
host, log file prefixes and path to a location to save the data.

**Command line arguments**: The command line options supersede the configuration file settings. Run help to list all
configurable parameters: `./register_stack.py -h`

**Authentication**: Basic auth is resolved automatically by the Requests library by reading a **~/.netrc** file. Make sure
to set up the correct entries (Sentinel and STAC host URL) there.
