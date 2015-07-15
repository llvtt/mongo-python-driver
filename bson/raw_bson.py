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

"""Tools for representing raw BSON documents.
"""

import collections

from bson import _UNPACK_INT, _element_to_dict, _ELEMENT_GETTER
from bson.errors import InvalidBSON
from bson.py3compat import iteritems
from bson.codec_options import (
    CodecOptions, DEFAULT_CODEC_OPTIONS, _RAW_BSON_DOCUMENT_MARKER)


class RawBSONIterator(collections.Iterator):
    """Representation for a MongoDB array that provides access to the raw
    BSON bytes that compose it.

    Documents are decoded as RawBSONDocuments. All other items are decoded
    non-lazily.
    """

    def __init__(self, data, codec_options=DEFAULT_CODEC_OPTIONS):
        self.__data = data
        # Automatically use RawBSONDocument as the document_class.
        self.__codec_options = CodecOptions(
            tz_aware=codec_options.tz_aware,
            document_class=RawBSONDocument,
            uuid_representation=codec_options.uuid_representation)
        self.__iterator = None

    @property
    def raw(self):
        return self.__data

    def next(self):
        if self.__iterator is None:
            self.__iterator = iter(self)
        return next(self.__iterator)

    __next__ = next

    def __iter__(self):
        list_size = _UNPACK_INT(self.__data[:4])[0]
        if len(self.__data) < list_size:
            raise InvalidBSON("invalid object size")
        list_end = list_size - 1
        if self.__data[list_end:list_size] != b"\x00":
            raise InvalidBSON("bad eoo")

        # Skip the document size header.
        position = 4
        while position < list_end:
            element_type = self.__data[position:position + 1]
            # Skip the keys.
            position = self.__data.index(b'\x00', position) + 1
            value, position = _ELEMENT_GETTER[element_type](
                self.__data, position, list_end, self.__codec_options)
            yield value

    def __cmp__(self, other):
        return cmp(list(self), list(other))

    def __getitem__(self):
        return NotImplemented


class RawBSONDocument(collections.Mapping):
    """Representation for a MongoDB document that provides access to the raw
    BSON bytes that compose it.

    Only when a field is accessed or modified within the document does
    RawBSONDocument decode its bytes. Similarly, only when the underlying bytes
    are accessed does RawBSONDocument re-encode any modifications made.
    """

    _type_marker = _RAW_BSON_DOCUMENT_MARKER
    _list_class = RawBSONIterator

    def __init__(self, bson_bytes, codec_options=DEFAULT_CODEC_OPTIONS):
        """Create a new :class:`RawBSONDocument`.

        :Parameters:
          - `bson_bytes`: the BSON bytes that compose this document
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`. RawBSONDocument does not
            respect the `document_class` attribute of `codec_options` when
            decoding its bytes and always uses a `dict` for this purpose.
        """
        self.__raw = bson_bytes
        self.__inflated_doc = None
        self.__codec_options = CodecOptions(
            tz_aware=codec_options.tz_aware,
            document_class=RawBSONDocument,
            uuid_representation=codec_options.uuid_representation)

    @property
    def raw(self):
        """The raw BSON bytes composing this document."""
        return self.__raw

    def _items(self):
        """Lazily decode and iterate elements in this document."""
        if self.__inflated_doc is not None:
            for name, value in iteritems(self.__inflated_doc):
                yield name, value
        else:
            obj_size = _UNPACK_INT(self.__raw[:4])[0]
            if len(self.__raw) < obj_size:
                raise InvalidBSON("invalid object size")
            obj_end = obj_size - 1
            if self.__raw[obj_end:obj_size] != b"\x00":
                raise InvalidBSON("bad eoo")

            # Skip the document size header.
            position = 4
            while position < obj_end:
                name, value, position = _element_to_dict(
                    self.raw, position, obj_size, self.__codec_options)
                yield name, value

    @property
    def __inflated(self):
        if self.__inflated_doc is None:
            self.__inflated_doc = dict(self._items())
        return self.__inflated_doc

    def __hasitem__(self, item):
        if self.__inflated_doc is not None:
            return item in self.__inflated
        for name, value in self._items():
            if name == item:
                return True
        return False

    def __getitem__(self, item):
        # TODO: decode certain items to RawBSONDocument and RawBSONList.
        return self.__inflated[item]

    def __iter__(self):
        return iter(self.__inflated)

    def __len__(self):
        return len(self.__inflated)

    def __cmp__(self, other):
        return cmp(self.__inflated, other)

    def __repr__(self):
        return repr(self.__inflated)
