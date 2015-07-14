import datetime
import uuid

import pymongo

from bson.binary import JAVA_LEGACY
from bson.codec_options import CodecOptions
from bson.raw_bson import RawBSONDocument
from test import client_context, unittest, pair


class TestRawBSONDocument(unittest.TestCase):

    # {u'_id': ObjectId('556df68b6e32ab21a95e0785'),
    #  u'name': u'Sherlock',
    #  u'address': {u'street': u'Baker Street'}}
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

    @client_context.require_connection
    def test_round_trip(self):
        client = pymongo.MongoClient(pair, document_class=RawBSONDocument)
        client.pymongo_test.test_raw.insert_one(self.document)
        result = client.pymongo_test.test_raw.find_one(self.document['_id'])
        self.assertIsInstance(result, RawBSONDocument)
        self.assertEqual(self.document, result)

    def test_with_codec_options(self):
        # {u'date': datetime.datetime(2015, 6, 3, 18, 40, 50, 826000),
        #  u'_id': UUID('026fab8f-975f-4965-9fbf-85ad874c60ff')}
        # encoded with JAVA_LEGACY uuid representation.
        bson_string = (
            b'-\x00\x00\x00\x05_id\x00\x10\x00\x00\x00\x03eI_\x97\x8f\xabo\x02'
            b'\xff`L\x87\xad\x85\xbf\x9f\tdate\x00\x8a\xd6\xb9\xbaM'
            b'\x01\x00\x00\x00'
        )
        document = RawBSONDocument(
            bson_string,
            codec_options=CodecOptions(uuid_representation=JAVA_LEGACY))

        self.assertEqual(uuid.UUID('026fab8f-975f-4965-9fbf-85ad874c60ff'),
                         document['_id'])

    @client_context.require_connection
    def test_round_trip_codec_options(self):
        doc = {
            'date': datetime.datetime(2015, 6, 3, 18, 40, 50, 826000),
            '_id': uuid.UUID('026fab8f-975f-4965-9fbf-85ad874c60ff')
        }
        db = pymongo.MongoClient(pair).pymongo_test
        coll = db.get_collection(
            'test_raw',
            codec_options=CodecOptions(uuid_representation=JAVA_LEGACY))
        coll.insert_one(doc)
        coll = db.get_collection(
            'test_raw',
            codec_options=CodecOptions(uuid_representation=JAVA_LEGACY,
                                       document_class=RawBSONDocument))
        self.assertEqual(doc, coll.find_one())
