from lxml import etree
from pymongo import MongoClient, GEO2D


class Gazetteer:

    XML = 'data/Gazetteer2012GML.gml'
    feature_codes = ['LOCB', 'LOCU', 'POPL', 'SUB', 'URBN']
    states = ['ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']

    def __init__(self):
        client = MongoClient()
        db = client.trove_places
        collection = db.places
        collection.ensure_index([('loc', GEO2D)], min=-500, max=500)
        self.db = db
        self.collection = collection

    def fast_iter(self, context, func, *args, **kwargs):
        """
        http://lxml.de/parsing.html#modifying-the-tree
        Based on Liza Daly's fast_iter
        http://www.ibm.com/developerworks/xml/library/x-hiperfparse/
        See also http://effbot.org/zone/element-iterparse.htm
        """
        for event, elem in context:
            func(elem, *args, **kwargs)
            # It's safe to call clear() here because no descendants will be
            # accessed
            elem.clear()
            # Also eliminate now-empty references from the root node to elem
            for ancestor in elem.xpath('ancestor-or-self::*'):
                while ancestor.getprevious() is not None:
                    del ancestor.getparent()[0]
        del context

    def get_feature_value(self, elem, feature):
        value = elem.find('{{http://www.safe.com/gml/fme}}{}'.format(feature)).text
        return value

    def process_element(self, elem):
        try:
            code = self.get_feature_value(elem, 'FEAT_CODE')
            authority = self.get_feature_value(elem, 'AUTHORITY_ID')
            if authority in self.states and code in self.feature_codes:
                place = {}
                place['_id'] = self.get_feature_value(elem, 'RECORD_ID')
                place['name'] = self.get_feature_value(elem, 'NAME')
                place['name_lower'] = self.get_feature_value(elem, 'NAME').lower()
                place['state'] = self.get_feature_value(elem, 'STATE_ID')
                place['fullname'] = '{}, {}'.format(place['name'].encode('utf-8').lower(), place['state'].encode('utf-8'))
                place['feature_code'] = code
                lon = float(self.get_feature_value(elem, 'LONGITUDE'))
                lat = float(self.get_feature_value(elem, 'LATITUDE'))
                place['loc'] = [lon, lat]
                self.collection.save(place)
                print place['name']
        except AttributeError:
            pass

    def hide_duplicates(self):
        for place in self.collection.find().distinct('fullname'):
            print place
            places = self.collection.find({'fullname': place}).count()
            if places > 0:
                for code in self.feature_codes:
                    dupe = self.collection.find_one({'fullname': place, 'feature_code': code})
                    if dupe:
                        dupe['display'] = 'y'
                        self.collection.save(dupe)
                        break
            else:
                place['display'] = 'y'
                self.collection.save(place)

    def load_data(self):
        context = etree.iterparse(self.XML, tag='{http://www.safe.com/gml/fme}GML')
        self.fast_iter(context, self.process_element)
