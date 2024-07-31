# DhusPytools
Python scripts for Sentinel Data Hub

## Catalogue_sync
Scripts for updating Sentinel and STAC catalogues.

### [get_new_list.py](./get_new_list.py)
Fetches recent products and stores a list of product IDs for further processing.

### [register_stac.py](./register_stac)
Fetches data from Sentinel Data Hub and generates STAC metadata. Pushes the data to a catalogue.
