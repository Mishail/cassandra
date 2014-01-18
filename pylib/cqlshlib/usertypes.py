# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from cql.marshal import uint16_unpack

USER_TYPE = '\'org.apache.cassandra.db.marshal.UserType\''


def _is_usertype(cls):
    return cls.typename == USER_TYPE


def _decode_ut_name(cls):
    return cls.subtypes[1].cassname.decode("hex")

#/cql.cqltypes._CassandraType#cql_parameterized_type
def cql_parameterized_type(cls):
    """
    Return a CQL type specifier for this type. If this type has parameters,
    they are included in standard CQL <> notation.
    """
    if _is_usertype(cls):
        return _decode_ut_name(cls)

    if not cls.subtypes:
        return cls.typename
    return '%s<%s>' % (cls.typename, ', '.join(styp.cql_parameterized_type() for styp in cls.subtypes))


def _deserialize(ut_dict, cls, byts):
    ksname = cls.subtypes[0].cassname
    utname = _decode_ut_name(cls)
    ks_dict = ut_dict.get(ksname, {})
    types_list = ks_dict.get(utname, [])
    if not types_list:
        return None

    p = 0
    result = []
    for col_name, col_type in types_list:
        if p == len(byts):
            break
        itemlen = uint16_unpack(byts[p:p + 2])
        p += 2
        item = byts[p:p + itemlen]
        p += itemlen
        result.append((col_name, col_type.from_binary(item)))
        p += 1

    return result

#cql.cqltypes._CassandraType#from_binary
def from_binary(ut_dict, cls, byts):
    """
    Deserialize a bytestring into a value. See the deserialize() method
    for more information. This method differs in that if None or the empty
    string is passed in, None may be returned.
    """
    if byts is None:
        return None
    if byts == '' and not cls.empty_binary_ok:
        return None
    if _is_usertype(cls):
        return _deserialize(ut_dict, cls, byts)
    return cls.deserialize(byts)


