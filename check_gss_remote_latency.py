# vim: ai et sw=4 ts=4
# Author: fous <honza801@gmail.com> 2023, 2025, 2026
from latency.products import GssSiteRegistry
#from latency.dhusparser import DhusConfig
import latency.ingester
import logging
from datetime import timedelta
import sys
import json
from HTTPAuthOptions import HTTPAuthRegistry
import argparse


class NagiosChecker:

    results = {}

    def __init__(self, config_local, thresholds, namespace):
        self.http_auth_registry = HTTPAuthRegistry()
        auth_local = self.http_auth_registry.add(config_local['auth'])

        self.site_registry = GssSiteRegistry()
        self.site_local = self.site_registry.get(config_local['serviceRootUrl'], auth=auth_local)

        self.warn = thresholds['warn']
        self.crit = thresholds['crit']
        self.unknown = thresholds['unknown']

        self.namespace = namespace

    """
    Walks through all active synchronizers in dhus config file (dhus.xml)
    measuring creation date of last element in local repository
    """
    def check_dhus_config(self, dconfig):
        dhusconfig = DhusConfig(dconfig)
        for syn in dhusconfig.get_active_synchronizers():
            self.site_local.load_xml({'filter': syn.get_filter()})
            loc_entry = self.site_local.get_first_entry()
            logging.debug(self.site_local)
            logging.debug(loc_entry)
        
            try:
                syn_products = Products(syn.get_url(), syn.get_login(), syn.get_password(), {'id': loc_entry.get_id()})
                syn_entry = syn_products.get_first_entry()
                logging.debug(syn_products)
                logging.debug(syn_entry)

                self.results[syn.get_label()] = loc_entry.get_creation_datetime() - syn_entry.get_creation_datetime()
            except:
                self.results[syn.get_label()] = timedelta(minutes=-1)
    
    """
    Check for the GSS services
    """
    def check_gss_product(self, product_type):
        self.site_local.load({'type': product_type})
        product_local = self.site_local.get_first_product()
        logging.debug(product_local)

        try:
            producer = latency.ingester.get_producer_entity_from_product_type(
                namespace=self.namespace,
                product_type=product_type.lower(),
            )
            
            auth = self.http_auth_registry.get_by_token_endpoint(producer['source']['auth'])
            site_remote = self.site_registry.get(producer['source']['serviceRootUrl'], auth)
            
            site_remote.load({'id': product_local.get_id()})
            product_remote = site_remote.get_first_product()
            logging.debug(product_remote)

            self.results[product_type] = product_local.compare_publication_date(product_remote)
        except Exception as e:
            self.results[product_type] = timedelta(minutes=-1)
            logging.debug(f'Error {e}')

    def format_result_output(self):
        ecode = 0
        msgs = []
        perfdata = []
        for stype, delta in self.results.items():
            if delta > self.crit:
                res = 'CRIT'
                if ecode < 2: ecode = 2
            elif delta > self.warn:
                res = 'WARN'
                if ecode < 1: ecode = 1
            elif delta < self.unknown:
                res = 'UNKNOWN'
                ecode = 3
            else:
                res = 'OK'
            msgs.append(f'{res} {stype}:[{int(delta.total_seconds()/60)}min]')
            perfdata.append('{}={}'.format(stype.replace(' ', '_'), int(delta.total_seconds()/60)))
        nagios_result = '{} | {}'.format(', '.join(msgs), ' '.join(perfdata))
        logging.debug(f'{nagios_result} | exit code:{ecode}')
        return (ecode, nagios_result)


def parse_args():
    p = argparse.ArgumentParser(description="GSS Latency monitoring")
    p.add_argument(
        "-c", "--config",
        default='latency/config.json',
        help="Path to JSON config file (default: %(default)s)"
    )
    p.add_argument(
        "-n", "--netrc",
        help="Path to netrc file (default: %(default)s)"
    )
    p.add_argument(
        "-d", "--debug",
        action="store_true",
        help="Enable debug output",
    )
    return p.parse_args()


if __name__ == '__main__':
    args = parse_args()
	
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        #filename='/var/dhus/latency-monitoring.log',
        format='%(asctime)s %(message)s')

    with open(args.config) as file:
        config = json.load(file)

    thresholds = {
        'warn': timedelta(hours=config['thresholds']['warnHours']),
        'crit': timedelta(hours=config['thresholds']['critHours']),
        'unknown': timedelta(hours=config['thresholds']['unknownHours'])
    }

    if args.netrc:
        NETRC_FILE = args.netrc
    else:
        NETRC_FILE = config.get("netrcFile")
    
    nag = NagiosChecker(config['local'], thresholds, config['kubernetes']['namespace'])
        
    for product_type in config['productTypes']:
        nag.check_gss_product(product_type)

    # Get the results
    (ecode, msgs) = nag.format_result_output()
    print(msgs)
    sys.exit(ecode)
