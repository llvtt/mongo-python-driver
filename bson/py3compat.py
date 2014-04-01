# -*- coding: utf-8 -*-
# Copyright 2009-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""Utility functions and definitions for python3 compatibility."""

import sys

PY3 = sys.version_info[0] == 3


class Iterator(object):
    def next(self):
        return self.__next__()

if PY3:
    import codecs
    from io import BytesIO as StringIO

    def b(s):
        # BSON and socket operations deal in binary data. In
        # python 3 that means instances of `bytes`. In python
        # 2.6 and 2.7 you can create an alias for `bytes` using
        # the b prefix (e.g. b'foo'). Python 2.4 and 2.5 don't
        # provide this marker so we provide this compat function.
        # In python 3.x b('foo') results in b'foo'.
        # See http://python3porting.com/problems.html#nicer-solutions
        return codecs.latin_1_encode(s)[0]

    def _unicode(s):
        '''Strings are unicode by default in python 3'''
        return s

    def u(s):
        return s

    def bytes_from_hex(h):
        return bytes.fromhex(h)

    def iteritems(d):
        return d.items()

    def reraise(exctype, value, trace=None):
        raise exctype(str(value)).with_traceback(trace)

    binary_type = bytes
    long_type   = int
    text_type   = str
    next_item   = "__next__"

else:
    try:
        from cStringIO import StringIO
    except ImportError:
        from StringIO import StringIO

    def b(s):
        # See comments above. In python 2.x b('foo') is just 'foo'.
        return s

    import codecs

    def _unicode(s):
        '''Replace builtin unicode function'''
        return unicode(s)

    def u(s):
        '''replacement for unicode literal prefix'''
        try:
            # s is valid UTF8: "W∞høô¡"
            return unicode(s, 'utf8')
        except ValueError:
            # s contains \x[0-9][0-9] escape codes: "revoluci\xf3n"
            return unicode(s.replace(r'\\', r'\\\\'), 'unicode_escape')

    def bytes_from_hex(h):
        return h.decode('hex')

    def iteritems(d):
        return d.iteritems()

    # "raise x, y, z" raises SyntaxError in Python 3
    exec("""
def reraise(exctype, value, trace=None):
    raise exctype, str(value), trace""")

    binary_type = str
    # 2to3 will convert this to "str". That's okay
    # since we won't ever get here under python3.
    long_type   = long
    text_type   = unicode
    next_item   = "next"

string_types = (binary_type, text_type)
integer_types = (long_type, int,)
