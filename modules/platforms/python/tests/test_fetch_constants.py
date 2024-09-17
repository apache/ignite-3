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
import decimal
import math
import uuid
import datetime

import pytest


test_data = [
    ("select 'Lorem ipsum'", 'Lorem ipsum'),
    ("select ''", ''),
    ("select 42::TINYINT", 42),
    ("select -18::TINYINT", -18),
    ("select 4242::SMALLINT", 4242),
    ("select 987654321", 987654321),
    ("select 1234567890987654321::BIGINT", 1234567890987654321),
    ("select 123.456::REAL", 123.456),
    ("select -123456789.987654321::DOUBLE", -123456789.987654321),
    ("select TRUE", True),
    ("select FALSE", False),
    ("select x'45F0AB'", b'\x45\xf0\xab'),
    ("select x''", b''),
    ("select NULL", None),
    ("SELECT 'c4a0327c-44be-416d-ae90-75c05079789f'::UUID", uuid.UUID('c4a0327c-44be-416d-ae90-75c05079789f')),
    ("SELECT '00000000-0000-0000-0000-000000000001'::UUID", uuid.UUID('00000000-0000-0000-0000-000000000001')),
    ("SELECT '10101010-1010-1010-1010-101010101010'::UUID", uuid.UUID('10101010-1010-1010-1010-101010101010')),
    ("SELECT DATE'1969-07-20'", datetime.date(1969, 7, 20)),
    ("SELECT DATE'1525-01-01'", datetime.date(1525, 1, 1)),
    ("SELECT DATE'2024-09-12'", datetime.date(2024, 9, 12)),
    ("SELECT TIME'20:17:40'", datetime.time(20, 17, 40)),
    ("SELECT TIME'07:59:13'", datetime.time(7, 59, 13)),
    ("SELECT TIME'00:00:00'", datetime.time(0, 0, 0)),
    ("SELECT TIMESTAMP'1969-07-20 20:17:40'", datetime.datetime(1969, 7, 20, 20, 17, 40)),
    ("SELECT TIMESTAMP'2024-09-12 07:59:13'", datetime.datetime(2024, 9, 12, 7, 59, 13)),
    ("SELECT TIMESTAMP'1000-01-01 00:00:00'", datetime.datetime(1000, 1, 1, 0, 0, 0)),
    ("SELECT TIMESTAMP'1000-01-01 00:00:00'", datetime.datetime(1000, 1, 1, 0, 0, 0)),
    ("SELECT '1111111111111111111111111111111'::DECIMAL", decimal.Decimal('1111111111111111111111111111111')),
    ("SELECT '11111111111111.11111111111111111'::DECIMAL(31,17)", decimal.Decimal('11111111111111.11111111111111111')),
    ("SELECT '0.000000000000000000000000000001'::DECIMAL(31,30)", decimal.Decimal('0.000000000000000000000000000001')),
    ("SELECT '123.456789'::DECIMAL(9,6)", decimal.Decimal('123.456789')),
    ("SELECT '-123.456789'::DECIMAL(9,6)", decimal.Decimal('-123.456789')),
    ("SELECT '1'::DECIMAL(20,10)", decimal.Decimal(1)),
    ("SELECT '0'::DECIMAL(20,10)", decimal.Decimal(0)),
    ("SELECT INTERVAL '0' DAY", datetime.timedelta(days=0)),
    ("SELECT INTERVAL '1' DAY", datetime.timedelta(days=1)),
    ("SELECT INTERVAL '145' DAY", datetime.timedelta(days=145)),
    ("SELECT INTERVAL '123456789' SECOND", datetime.timedelta(seconds=123456789)),
    ("SELECT INTERVAL '987654321.123' SECOND", datetime.timedelta(seconds=987654321, milliseconds=123)),
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
