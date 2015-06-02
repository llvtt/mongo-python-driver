from test import client_context, unittest, pair

import pymongo

from bson.raw_bson_document import RawBSONDocument


class TestRawBSONDocument(unittest.TestCase):

    # {u'_id': ObjectId('556df68b6e32ab21a95e0785'),
    # u'name': u'Sherlock',
    # u'address': {u'street': u'Baker Street'}}
    bson_string = (b'P\x00\x00\x00\x07_id\x00Um\xf6\x8bn2\xab!\xa9^\x07\x85'
                   b'\x02name\x00\t\x00\x00\x00Sherlock\x00\x03address\x00'
                   b'\x1e\x00\x00\x00\x02street\x00\r\x00\x00\x00'
                   b'Baker Street\x00\x00\x00')
    document = RawBSONDocument(bson_string)

    def tearDown(self):
        if client_context.connected:
            client_context.client.pymongo_test.test_raw.drop()

    def test_decode(self):
        self.assertEqual('Sherlock', self.document['name'])
        self.assertIsInstance(self.document['address'], dict)
        self.assertEqual('Baker Street', self.document['address']['street'])

    def test_raw(self):
        self.assertEqual(self.bson_string, self.document.raw)

    def test_setitem(self):
        document = RawBSONDocument(self.bson_string)
        document['foo'] = 'bar'
        self.assertEqual(
            (b']\x00\x00\x00\x07_id\x00Um\xf6\x8bn2\xab!\xa9^\x07\x85\x02foo'
             b'\x00\x04\x00\x00\x00bar\x00\x02name\x00\t\x00\x00\x00Sherlock'
             b'\x00\x03address\x00\x1e\x00\x00\x00\x02street\x00\r\x00\x00'
             b'\x00Baker Street\x00\x00\x00'),
            document.raw
        )
        self.assertEqual(4, len(document))

    def test_delitem(self):
        document = RawBSONDocument(self.bson_string)
        self.assertEqual('Sherlock', document.pop('name'))
        self.assertEqual(2, len(document))
        self.assertEqual(
            (b'=\x00\x00\x00\x07_id\x00Um\xf6\x8bn2\xab!\xa9^\x07\x85'
             b'\x03address\x00\x1e\x00\x00\x00\x02street\x00\r\x00\x00'
             b'\x00Baker Street\x00\x00\x00'),
            document.raw
        )

    @client_context.require_connection
    def test_round_trip(self):
        client = pymongo.MongoClient(pair, document_class=RawBSONDocument)
        client.pymongo_test.test_raw.insert_one(self.document)
        result = client.pymongo_test.test_raw.find_one(self.document['_id'])
        self.assertIsInstance(result, RawBSONDocument)
        self.assertEqual(self.document, result)
