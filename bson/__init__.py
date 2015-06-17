# Copyright 2009-2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""BSON (Binary JSON) encoding and decoding.
"""

try:
    from bson._cbson import (
        decode_all, _bson_to_dict, _dict_to_bson, _element_to_dict)
    _USE_C = True
except ImportError:
    _USE_C = False
    from bson.pybson import (
        decode_all, _bson_to_dict, _dict_to_bson, _element_to_dict)

from bson.pybson import (
    EPOCH_AWARE,
    EPOCH_NAIVE,
    BSONNUM,
    BSONSTR,
    BSONOBJ,
    BSONARR,
    BSONBIN,
    BSONUND,
    BSONOID,
    BSONBOO,
    BSONDAT,
    BSONNUL,
    BSONRGX,
    BSONREF,
    BSONCOD,
    BSONSYM,
    BSONCWS,
    BSONINT,
    BSONTIM,
    BSONLON,
    BSONMIN,
    BSONMAX,

    RawBSONDocument,

    is_valid,
    gen_list_name,
    decode_iter,
    decode_file_iter,

    Binary,
    OLD_UUID_SUBTYPE,
    JAVA_LEGACY,
    CSHARP_LEGACY,
    UUIDLegacy,
    CodecOptions,
    DEFAULT_CODEC_OPTIONS,
    DBRef,
    InvalidBSON,
    InvalidDocument,
    InvalidStringData,
    Int64,
    MaxKey,
    MinKey,
    ObjectId,
    b,
    PY3,
    iteritems,
    text_type,
    string_type,
    reraise,
    Regex,
    SON,
    RE_TYPE,
    Timestamp,
    utc
)


class BSON(bytes):
    """BSON (Binary JSON) data.
    """

    @classmethod
    def encode(cls, document, check_keys=False,
               codec_options=DEFAULT_CODEC_OPTIONS):
        """Encode a document to a new :class:`BSON` instance.

        A document can be any mapping type (like :class:`dict`).

        Raises :class:`TypeError` if `document` is not a mapping type,
        or contains keys that are not instances of
        :class:`basestring` (:class:`str` in python 3). Raises
        :class:`~bson.errors.InvalidDocument` if `document` cannot be
        converted to :class:`BSON`.

        :Parameters:
          - `document`: mapping type representing a document
          - `check_keys` (optional): check if keys start with '$' or
            contain '.', raising :class:`~bson.errors.InvalidDocument` in
            either case
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`.

        .. versionchanged:: 3.0
           Replaced `uuid_subtype` option with `codec_options`.
        """
        if not isinstance(codec_options, CodecOptions):
            TypeError("codec_options must be an instance of CodecOptions")

        return cls(_dict_to_bson(document, check_keys, codec_options))

    def decode(self, codec_options=DEFAULT_CODEC_OPTIONS):
        """Decode this BSON data.

        By default, returns a BSON document represented as a Python
        :class:`dict`. To use a different :class:`MutableMapping` class,
        configure a :class:`~bson.codec_options.CodecOptions`::

            >>> import collections  # From Python standard library.
            >>> import bson
            >>> from bson.codec_options import CodecOptions
            >>> data = bson.BSON.encode({'a': 1})
            >>> decoded_doc = bson.BSON.decode(data)
            <type 'dict'>
            >>> options = CodecOptions(document_class=collections.OrderedDict)
            >>> decoded_doc = bson.BSON.decode(data, codec_options=options)
            >>> type(decoded_doc)
            <class 'collections.OrderedDict'>

        :Parameters:
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`.

        .. versionchanged:: 3.0
           Removed `compile_re` option: PyMongo now always represents BSON
           regular expressions as :class:`~bson.regex.Regex` objects. Use
           :meth:`~bson.regex.Regex.try_compile` to attempt to convert from a
           BSON regular expression to a Python regular expression object.

           Replaced `as_class`, `tz_aware`, and `uuid_subtype` options with
           `codec_options`.

        .. versionchanged:: 2.7
           Added `compile_re` option. If set to False, PyMongo represented BSON
           regular expressions as :class:`~bson.regex.Regex` objects instead of
           attempting to compile BSON regular expressions as Python native
           regular expressions, thus preventing errors for some incompatible
           patterns, see `PYTHON-500`_.

        .. _PYTHON-500: https://jira.mongodb.org/browse/PYTHON-500
        """
        if not isinstance(codec_options, CodecOptions):
            TypeError("codec_options must be an instance of CodecOptions")

        return _bson_to_dict(self, codec_options)


def has_c():
    """Is the C extension installed?
    """
    return _USE_C
