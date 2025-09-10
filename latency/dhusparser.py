# vim: ai et sw=4 ts=4
import xml.etree.ElementTree as ET
import logging


class DhusConfig:
    
    namespace = 'fr.gael.dhus.database.object.config'

    def __init__(self, xmlfile):
        self.xmltree = ET.parse(xmlfile)
        self.xmlroot = self.xmltree.getroot()

    def get_active_synchronizers(self):
        filtered = []
        synchronizers = self.xmlroot.findall(f'./{{{self.namespace}}}synchronizers/')
        for synchronizer in synchronizers:
            new_synch = Synchronizer(synchronizer)
            if new_synch.is_active():
                filtered.append(Synchronizer(synchronizer))
        return filtered


class Synchronizer:

    namespace = 'fr.gael.dhus.database.object.config.synchronizer'
    
    def __init__(self, xmlpart):
        self.xmlpart = xmlpart

    def get_label(self):
        return self.xmlpart.find(f'./{{{self.namespace}}}label').text

    def get_url(self):
        return self.xmlpart.find(f'./{{{self.namespace}}}serviceUrl').text

    def get_login(self):
        return self.xmlpart.find(f'./{{{self.namespace}}}serviceLogin').text

    def get_password(self):
        return self.xmlpart.find(f'./{{{self.namespace}}}servicePassword').text

    def is_active(self):
        return self.xmlpart.find(f'./{{{self.namespace}}}active').text in ['True', 'true']

    def get_filter(self):
        return self.xmlpart.find(f'./{{{self.namespace}}}filterParam').text

    def __repr__(self):
        return f'label:{self.get_label()} active:{self.is_active()}'


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    dhusconfig = DhusConfig('dhus-be1.xml')
    logging.debug(dhusconfig.get_active_synchronizers())
