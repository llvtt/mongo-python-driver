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
    from bson import _cbson
    from bson._cbson import decode_all
    _USE_C = True
except ImportError:
    _USE_C = False
    from bson.pybson import decode_all

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
    BSON,

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


def has_c():
    """Is the C extension installed?
    """
    return _USE_C
