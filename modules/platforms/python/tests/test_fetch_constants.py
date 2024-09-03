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
from tests.util import start_cluster_gen, check_cluster_started, server_addresses_invalid, server_addresses_basic


@pytest.fixture(autouse=True)
def cluster():
    if not check_cluster_started():
        yield from start_cluster_gen()
    else:
        yield None


def test_fetch_constant_string():
    with pyignite3.connect(address=server_addresses_basic[0]) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select 'Lorem ipsum'")
            data = cursor.fetchone()
            assert len(data) == 1
            assert data[0] == 'Lorem ipsum'


def test_fetch_constant_string_empty():
    with pyignite3.connect(address=server_addresses_basic[0]) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select ''")
            data = cursor.fetchone()
            assert len(data) == 1
            assert data[0] == ''


def test_fetch_constant_tinyint():
    with pyignite3.connect(address=server_addresses_basic[0]) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select CAST(42 AS TINYINT)")
            data = cursor.fetchone()
            assert len(data) == 1
            assert data[0] == 42


def test_fetch_constant_tinyint_negative():
    with pyignite3.connect(address=server_addresses_basic[0]) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select CAST(-18 AS TINYINT)")
            data = cursor.fetchone()
            assert len(data) == 1
            assert data[0] == -18


def test_fetch_constant_smallint():
    with pyignite3.connect(address=server_addresses_basic[0]) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select CAST(4242 AS SMALLINT)")
            data = cursor.fetchone()
            assert len(data) == 1
            assert data[0] == 4242


def test_fetch_constant_int():
    with pyignite3.connect(address=server_addresses_basic[0]) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select 987654321")
            data = cursor.fetchone()
            assert len(data) == 1
            assert data[0] == 987654321


def test_fetch_constant_bigint():
    with pyignite3.connect(address=server_addresses_basic[0]) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select CAST(1234567890987654321 AS BIGINT)")
            data = cursor.fetchone()
            assert len(data) == 1
            assert data[0] == 1234567890987654321


def test_fetch_constant_real():
    with pyignite3.connect(address=server_addresses_basic[0]) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select CAST(123.456 AS REAL)")
            data = cursor.fetchone()
            assert len(data) == 1
            assert data[0] == pytest.approx(123.456)


def test_fetch_constant_double():
    with pyignite3.connect(address=server_addresses_basic[0]) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select CAST(-123456789.987654321 AS DOUBLE)")
            data = cursor.fetchone()
            assert len(data) == 1
            assert data[0] == pytest.approx(-123456789.987654321)


def test_fetch_constant_double_nan():
    with pyignite3.connect(address=server_addresses_basic[0]) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select CAST('NaN' AS DOUBLE)")
            data = cursor.fetchone()
            assert len(data) == 1
            assert math.isnan(data[0])


def test_fetch_constant_bool_true():
    with pyignite3.connect(address=server_addresses_basic[0]) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select TRUE")
            data = cursor.fetchone()
            assert len(data) == 1
            assert data[0] is True


def test_fetch_constant_bool_false():
    with pyignite3.connect(address=server_addresses_basic[0]) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select FALSE")
            data = cursor.fetchone()
            assert len(data) == 1
            assert data[0] is False


def test_fetch_constant_binary():
    with pyignite3.connect(address=server_addresses_basic[0]) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select x'45F0AB'")
            data = cursor.fetchone()
            assert len(data) == 1
            assert data[0] == b'\x45\xf0\xab'


def test_fetch_constant_binary_empty():
    with pyignite3.connect(address=server_addresses_basic[0]) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select x''")
            data = cursor.fetchone()
            assert len(data) == 1
            assert data[0] == b''


def test_fetch_constant_null():
    with pyignite3.connect(address=server_addresses_basic[0]) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select NULL")
            data = cursor.fetchone()
            assert len(data) == 1
            assert data[0] is None


def test_fetch_constant_several_ints():
    with pyignite3.connect(address=server_addresses_basic[0]) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select 1,2,3")
            data = cursor.fetchone()
            assert len(data) == 3
            assert data[0] == 1
            assert data[1] == 2
            assert data[2] == 3


def test_fetch_constant_int_bool_string():
    with pyignite3.connect(address=server_addresses_basic[0]) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select 42, TRUE, 'Test string'")
            data = cursor.fetchone()
            assert len(data) == 3
            assert data[0] == 42
            assert data[1] is True
            assert data[2] == 'Test string'

            nothing = cursor.fetchone()
            assert nothing is None
