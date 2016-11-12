import decimal

from bson import BSON, decode_all
from bson.codec_options import (
    BsonTransformer, TransformerRegistry, CodecOptions)
from bson.decimal128 import Decimal128
from bson.son import SON

from test import unittest


class DecimalTransformer(BsonTransformer):
    """Bidirectional transformer Decimal128 <--> decimal.Decimal"""
    def bson_type(self):
        return Decimal128

    def python_type(self):
        return decimal.Decimal

    def transform_bson(self, value):
        return value.to_decimal()

    def transform_python(self, value):
        return Decimal128(value)


class DecimalToD128(BsonTransformer):
    """Unidirectional transformer decimal.Decimal -> Decimal128"""
    def python_type(self):
        return decimal.Decimal

    def transform_python(self, value):
        return Decimal128(value)


class UselessWrapper(object):
    """Useless wrapper around any object."""
    def __init__(self, data):
        self.data = data

    def __eq__(self, other):
        return self.data == other.data


class UselessWrapperTransformer(BsonTransformer):
    """Bidirectional transformer UselessWrapper <--> anything"""
    def python_type(self):
        return UselessWrapper

    def bson_type(self):
        return dict

    def transform_python(self, value):
        return value.data

    def transform_bson(self, value):
        return UselessWrapper(value)


class BsonTransformerTestCase(unittest.TestCase):

    def gen_document(self, value):
        return {
            'top level': value,
            'in a list': [value],
            'subdocument': {'decimal': value},
            'subdocument in a list': [
                {'decimal': value}
            ]
        }

    def test_transform(self):
        opts = CodecOptions(
            transformer_registry=TransformerRegistry(DecimalTransformer()))
        document = self.gen_document(decimal.Decimal('3.14159'))
        decoded = BSON.encode(
            document, codec_options=opts).decode(codec_options=opts)
        self.assertEqual(document, decoded)

    def test_unidirectional(self):
        document = self.gen_document(decimal.Decimal('3.14159'))
        opts = CodecOptions(
            transformer_registry=TransformerRegistry(DecimalToD128()))
        decoded = BSON.encode(
            document, codec_options=opts).decode(codec_options=opts)
        expected = self.gen_document(Decimal128('3.14159'))
        self.assertEqual(expected, decoded)

    def test_with_document_class(self):
        opts = CodecOptions(
            transformer_registry=TransformerRegistry(DecimalTransformer()),
            document_class=SON)
        document = SON(**self.gen_document(decimal.Decimal('3.14159')))
        decoded = BSON.encode(
            document, codec_options=opts).decode(codec_options=opts)
        self.assertEqual(document, decoded)

    def test_transform_top_level(self):
        opts = CodecOptions(
            transformer_registry=TransformerRegistry(
                UselessWrapperTransformer()))
        document = UselessWrapper({'hello': 'world'})
        decoded = BSON.encode(
            document, codec_options=opts).decode(codec_options=opts)
        self.assertEqual(document, decoded)

    def test_decode_all(self):
        opts = CodecOptions(
            transformer_registry=TransformerRegistry(
                UselessWrapperTransformer()))
        document = UselessWrapper({'hello': 'world'})
        encoded = BSON.encode(document, codec_options=opts)
        decoded = decode_all(encoded, codec_options=opts)[0]
        self.assertEqual(document, decoded)
