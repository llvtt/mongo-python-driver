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

from bson import BSON
from bson.codec_options import (
    CodecOptions, DEFAULT_CODEC_OPTIONS, _RAW_BSON_DOCUMENT_MARKER)


class RawBSONDocument(collections.Mapping):
    """Representation for a MongoDB document that provides access to the raw
    BSON bytes that compose it.

    Only when a field is accessed or modified within the document does
    RawBSONDocument decode its bytes. Similarly, only when the underlying bytes
    are accessed does RawBSONDocument re-encode any modifications made.
    """

    _type_marker = _RAW_BSON_DOCUMENT_MARKER

    def __init__(self, bson_bytes, codec_options=DEFAULT_CODEC_OPTIONS):
        """Create a new :class:`RawBSONDocument`.

        :Parameters:
          - `bson_bytes`: the BSON bytes that compose this document
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`. RawBSONDocument does not
            respect the `document_class` attribute of `codec_options` when
            decoding its bytes and always uses a `dict` for this purpose.
        """
        self.__raw = BSON(bson_bytes)
        self.__inflated_doc = None
        # Don't let codec_options use RawBSONDocument as the document_class.
        self.__codec_options = CodecOptions(
            tz_aware=codec_options.tz_aware,
            uuid_representation=codec_options.uuid_representation)

    @property
    def raw(self):
        """The raw BSON bytes composing this document."""
        return self.__raw

    @property
    def __inflated(self):
        if self.__inflated_doc is None:
            self.__inflated_doc = self.__raw.decode(self.__codec_options)
        return self.__inflated_doc

    def __getitem__(self, item):
        return self.__inflated[item]

    def __iter__(self):
        return iter(self.__inflated)

    def __len__(self):
        return len(self.__inflated)

    def __cmp__(self, other):
        return cmp(self.__inflated, other)

    def __repr__(self):
        return repr(self.__inflated)
