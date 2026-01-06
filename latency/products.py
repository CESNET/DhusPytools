# vim: ai et sw=4 ts=4
import requests
import urllib
import logging
import html
import xml.etree.ElementTree as ET
from datetime import datetime
import json
from HTTPAuthOptions import KeycloakTokenAuth

class GssProducts:
    
    def __init__(self, url, auth=None, product={}):
        self.url = url+'/Products'
        self.auth = auth
        if product:
            self.load(product)

    def load(self, product={}):
        data = {
            '$filter': self._get_filter_from_product(product),
            '$orderby': 'PublicationDate desc',
            '$top': '1',
        }
        response = requests.get(
            #self._build_url(data),
            self.url,
            params=urllib.parse.urlencode(data, quote_via=urllib.parse.quote),
            auth=self.auth,
            headers={"Accept": "application/json"},
            )
        logging.debug(response.text)
        try:
            response_json = response.json()
            self.products = response_json['value']
        except:
            logging.critical(f'Error parsing json document {self.url}')
            raise
    
    def _build_url(self, data):
        query = '&'.join((f'{k}={v}' for k, v in data.items()))
        return f'{self.url}?{query}'

    def _get_filter_from_product(self, product):
        if 'type' in product:
            return f"startswith(Name,'{product['type']}')"
        elif 'id' in product:
            return f"Id eq {product['id']}"
        elif 'filter' in product:
            return product['filter']

    def get_first_product(self):
        if len(self.products) > 0:
            return GssProduct(self.products[0])
        return None


class GssProduct:

    def __init__(self, jsonpart):
        self.jsonpart = jsonpart

    def get_id(self):
        return self.jsonpart.get('Id')

    def get_publication_date(self):
        return datetime.fromisoformat(self.jsonpart.get('PublicationDate'))
    
    def compare_publication_date(self, product):
        return self.get_publication_date() - product.get_publication_date()

    def __repr__(self):
        return f'Id:{self.get_id()} PublicationDate:{self.get_publication_date()}'


class DhusProducts:

    entryns = 'http://www.w3.org/2005/Atom'

    def __init__(self, url, auth, product={}):
        self.url = url+'/Products'
        self.auth = auth
        if product:
            self.load_xml(product)

    def load_xml(self, product={}):
        data = {
            '$filter': self._get_filter_from_product(product),
            '$orderby': 'IngestionDate desc',
            '$top': '1',
        }
        r = requests.get(self.url, auth=(self.auth['user'], self.auth['password']), params=data)
        logging.debug(f'request output: {r.text}')
        self.xmltext = html.unescape(r.text)
        try:
            self.xmlroot = ET.fromstring(self.xmltext)
        except:
            logging.critical(f'Error parsing xml document {self.url}')
            raise

    def _get_filter_from_product(self, product):
        if 'type' in product:
            return f"startswith(Name,'{product['type']}')"
        elif 'id' in product:
            return f"Id eq '{product['id']}'"
        elif 'filter' in product:
            return product['filter']

    def get_first_entry(self):
        return DhusEntry(self.xmlroot.find(f'{{{self.entryns}}}entry'))
    
    def __repr__(self):
        return f'url:{self.url}'


class DhusEntry:

    propsns = 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata'
    datans = 'http://schemas.microsoft.com/ado/2007/08/dataservices'

    def __init__(self, xmlpart):
        self.xmlpart = xmlpart

    def get_id(self):
        eid = self.xmlpart.find(f'{{{self.propsns}}}properties/{{{self.datans}}}Id')
        return eid.text

    def get_ingestion_datetime(self):
        ig = self.xmlpart.find(f'{{{self.propsns}}}properties/{{{self.datans}}}IngestionDate')
        return datetime.fromisoformat(ig.text)

    def get_creation_datetime(self):
        ig = self.xmlpart.find(f'{{{self.propsns}}}properties/{{{self.datans}}}CreationDate')
        return datetime.fromisoformat(ig.text)

    def __repr__(self):
        return f'Id:{self.get_id()} IngestionDate:{self.get_ingestion_datetime()} CreationDate:{self.get_creation_datetime()}'


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    with open('config.json') as file:
        config = json.load(file)

    local_config = config['local']
    # Auth using KeycloakTokenAuth
    AUTH = KeycloakTokenAuth(
        server_url=local_config['auth']['tokenEndpoint'],
        realm=local_config['auth']['realm'],
        client_id=local_config['auth']['clientId'],
        #netrc_file=NETRC_FILE,
    )
    products_local = GssProducts(local_config['serviceRootUrl'], AUTH)
    products_local.load({'type':'S3A'})
    e = products_local.get_first_product()
    logging.info(products_local)
    logging.info(e)
