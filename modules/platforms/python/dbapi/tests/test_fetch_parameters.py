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

import pyignite_dbapi
import pytest

test_data = [
    0,
    1,
    -1,
    2,
    43,
    -543656,
    423538409739,
    0.0,
    123.456,
    -76.4,
    4235384097394235384097394235384097394235.38409739423538409739423538409739423538409739,
    1.0E-40,
    1.0E40,
    'test',
    'TEST',
    'Lorem Ipsum',
    '你好',
    'Мир!',
    '',
    True,
    False,
    None,
    b'',
    b'0',
    b'123456789',
    b'h9832y9r8wf08hw85h0h2508h0858',
    b'\x45\xf0\xab',
    pyignite_dbapi.Binary('Lorem Ipsum'),
    pyignite_dbapi.Binary('x' * 1234),
    pyignite_dbapi.UUID('c4a0327c-44be-416d-ae90-75c05079789f'),
    pyignite_dbapi.UUID('00000000-0000-0000-0000-000000000001'),
    pyignite_dbapi.UUID('10101010-1010-1010-1010-101010101010'),
    pyignite_dbapi.UUID(int=0),
    pyignite_dbapi.Time(23, 59, 59),
    pyignite_dbapi.Time(20, 17, 40),
    pyignite_dbapi.Time(7, 59, 13),
    pyignite_dbapi.TIME(1, 2, 3),
    pyignite_dbapi.TIME(0, 0, 0),
    pyignite_dbapi.TimeFromTicks(0),
    pyignite_dbapi.TimeFromTicks(89565),
    pyignite_dbapi.Date(1969, 7, 20),
    pyignite_dbapi.Date(1525, 1, 1),
    pyignite_dbapi.DATE(2024, 9, 12),
    pyignite_dbapi.DateFromTicks(0),
    pyignite_dbapi.DateFromTicks(8956872365),
    pyignite_dbapi.DATETIME(1969, 7, 20, 20, 17, 40),
    pyignite_dbapi.DATETIME(2024, 9, 12, 7, 59, 13),
    pyignite_dbapi.DATETIME(1000, 1, 1, 0, 0, 0),
    pyignite_dbapi.DATETIME(1000, 1, 1, 0, 0, 0),
    pyignite_dbapi.Timestamp(1979, 7, 20, 20, 17, 40),
    pyignite_dbapi.Timestamp(2024, 9, 12, 7, 59, 13),
    pyignite_dbapi.Timestamp(3000, 1, 1, 0, 0, 0),
    pyignite_dbapi.Timestamp(2007, 1, 1, 0, 0, 0),
    pyignite_dbapi.TimestampFromTicks(4239085792.333),
    # TODO: IGNITE-17373 Fix DURATION type parameters in select statements
    # pyignite_dbapi.DURATION(days=0),
    # pyignite_dbapi.DURATION(days=1),
    # pyignite_dbapi.DURATION(days=145),
    # pyignite_dbapi.DURATION(seconds=123456789),
    # pyignite_dbapi.DURATION(seconds=987654321, milliseconds=123),
    # pyignite_dbapi.DURATION(days=145, seconds=987654321, milliseconds=123),
]


def check_fetch_parameters(cursor, param, use_tuple: bool):
    cursor.execute("select ?", (param,) if use_tuple else [param])
    data = cursor.fetchone()
    assert len(data) == 1
    if isinstance(param, float):
        assert data[0] == pytest.approx(param)
    else:
        assert data[0] == param


@pytest.mark.parametrize("param", test_data)
def test_fetch_parameter_list(cursor, param):
    check_fetch_parameters(cursor, param, False)


@pytest.mark.parametrize("param", test_data)
def test_fetch_parameter_tuple(cursor, param):
    check_fetch_parameters(cursor, param, True)


test_decs = [
    pyignite_dbapi.NUMBER('1111111111111111111111111111111'),
    pyignite_dbapi.NUMBER('11111111111111.11111111111111111'),
    pyignite_dbapi.NUMBER('0.000000000000000000000000000001'),
    pyignite_dbapi.NUMBER('123.456789'),
    pyignite_dbapi.NUMBER('-123.456789'),
    pyignite_dbapi.NUMBER('2980949468541866002980035546865281241479836693504'),
    pyignite_dbapi.NUMBER('2980949468541866002980035546865281241479836693504.943353696379651248255943353696379651248255'),
    pyignite_dbapi.NUMBER('298094946854186600298003554686528124147.9836693504943353696379651248255'),
    pyignite_dbapi.NUMBER('298094946854186600298003554686528.1241479836693504943353696379651248255'),
    pyignite_dbapi.NUMBER('29809494685418660029800.35546865281241479836693504943353696379651248255'),
]


def check_fetch_decimals(cursor, param: decimal.Decimal, use_tuple: bool):
    cursor.execute(f"select ?::DECIMAL(100,50)", (param,) if use_tuple else [param])
    data = cursor.fetchone()
    assert len(data) == 1
    if isinstance(param, float):
        assert data[0] == pytest.approx(param)
    else:
        assert data[0] == param


@pytest.mark.parametrize("param", test_decs)
def test_fetch_decimals_list(cursor, param):
    check_fetch_decimals(cursor, param, False)


@pytest.mark.parametrize("param", test_decs)
def test_fetch_decimals_tuple(cursor, param):
    check_fetch_decimals(cursor, param, True)
