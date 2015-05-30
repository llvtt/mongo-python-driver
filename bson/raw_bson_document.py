# Copyright 2015 MongoDB, Inc.
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

"""BSONDocument class for representing raw BSON documents."""

import collections

import bson


# TODO: docstrings and stuff.
class RawBSONDocument(collections.Mapping):

    _raw = True

    def __init__(self, bson_bytes,
                 codec_options=bson.codec_options.DEFAULT_CODEC_OPTIONS):
        self.__raw = bson.BSON(bson_bytes)
        self.__inflated_doc = None
        self.__codec_options = codec_options

    @property
    def raw(self):
        return self.__raw

    @property
    def __inflated(self):
        if self.__inflated_doc is None:
            self.__inflated_doc = self.__raw.decode(self.__codec_options)
        return self.__inflated_doc

    def __getitem__(self, item):
        return self.__inflated[item]

    def __len__(self):
        # TODO: don't need to inflate for this.
        return len(self.__inflated)

    def __iter__(self):
        return iter(self.__inflated)
