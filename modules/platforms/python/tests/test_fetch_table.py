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
from tests.util import start_cluster_gen, check_cluster_started, server_addresses_basic


@pytest.fixture(autouse=True)
def cluster():
    if not check_cluster_started():
        yield from start_cluster_gen()
    else:
        yield None


def test_fetch_table_empty():
    table_name = test_fetch_table_empty.__name__
    with pyignite3.connect(address=server_addresses_basic[0]) as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute(f'create table {table_name}(id int primary key, col1 varchar)')
                cursor.execute(f"select col1, id from {table_name}")
                end = cursor.fetchone()
                assert end is None

            finally:
                cursor.execute(f'drop table if exists {table_name}')


def test_fetch_table_many_rows():
    table_name = test_fetch_table_many_rows.__name__
    rows_num = 15
    with pyignite3.connect(address=server_addresses_basic[0]) as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute(f'create table {table_name}(id int primary key, data varchar, fl double)')

                for i in range(rows_num):
                    cursor.execute(f"insert into {table_name} values ({i}, 'Value-{i*2}', {i / 2.0})")

                cursor.execute(f"select id, data, fl from {table_name} order by id")

                for i in range(rows_num):
                    row = cursor.fetchone()
                    assert len(row) == 3
                    assert row[0] == i
                    assert row[1] == f'Value-{i*2}'
                    assert row[2] == pytest.approx(i / 2.0)

                end = cursor.fetchone()
                assert end is None

            finally:
                cursor.execute(f'drop table if exists {table_name}')


