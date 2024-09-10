# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import math

import pytest

import pyignite3
from tests.util import server_addresses_basic


test_data = [
    ("select 'Lorem ipsum'", 'Lorem ipsum'),
    ("select ''", ''),
    ("select CAST(42 AS TINYINT)", 42),
    ("select CAST(-18 AS TINYINT)", -18),
    ("select CAST(4242 AS SMALLINT)", 4242),
    ("select 987654321", 987654321),
    ("select CAST(1234567890987654321 AS BIGINT)", 1234567890987654321),
    ("select CAST(123.456 AS REAL)", 123.456),
    ("select CAST(-123456789.987654321 AS DOUBLE)", -123456789.987654321),
    ("select TRUE", True),
    ("select FALSE", False),
    ("select x'45F0AB'", b'\x45\xf0\xab'),
    ("select x''", b''),
    ("select NULL", None),
]


@pytest.mark.parametrize("query,value", test_data)
def test_fetch_constant(query, value, cursor):
    cursor.execute(query)
    data = cursor.fetchone()
    assert len(data) == 1
    if isinstance(value, float):
        assert data[0] == pytest.approx(value)
    else:
        assert data[0] == value


def test_fetch_constant_double_nan(cursor):
    cursor.execute("select CAST('NaN' AS DOUBLE)")
    data = cursor.fetchone()
    assert len(data) == 1
    assert math.isnan(data[0])


def test_fetch_constant_several_ints(cursor):
    cursor.execute("select 1,2,3")
    data = cursor.fetchone()
    assert len(data) == 3
    assert data[0] == 1
    assert data[1] == 2
    assert data[2] == 3


def test_fetch_constant_int_bool_string(cursor):
    cursor.execute("select 42, TRUE, 'Test string'")
    data = cursor.fetchone()
    assert len(data) == 3
    assert data[0] == 42
    assert data[1] is True
    assert data[2] == 'Test string'

    nothing = cursor.fetchone()
    assert nothing is None
