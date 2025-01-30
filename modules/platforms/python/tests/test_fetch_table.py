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
import pytest

from tests.conftest import TEST_PAGE_SIZE

TEST_ROWS_NUM = 15

def get_row(index):
    return (index, f'Value-{index * 2}', index / 2.0)


def row_generator(begin, rows_num):
    for i in range(begin, begin + rows_num):
        yield get_row(i)


def create_and_populate_test_table(cursor, rows_num, table_name, batch_size=1):
    cursor.execute(f'drop table if exists {table_name}')
    cursor.execute(f'create table {table_name}(id int primary key, data varchar, fl double)')
    if batch_size == 1:
        for i in range(rows_num):
            cursor.execute(f"insert into {table_name} values (?, ?, ?)", params=get_row(i))
    else:
        batch = 0
        for batch in range(rows_num // batch_size):
            cursor.executemany(f"insert into {table_name} values(?, ?, ?)",
                               list(row_generator(batch * batch_size, batch_size)))
        if rows_num % batch_size:
            cursor.executemany(f"insert into {table_name} values(?, ?, ?)",
                               list(row_generator(batch * batch_size, rows_num % batch_size)))


def check_row(i, row):
    assert len(row) == 3
    assert row[0] == i
    assert row[1] == f'Value-{i * 2}'
    assert row[2] == pytest.approx(i / 2.0)


def test_fetchone_table_empty(table_name, cursor, drop_table_cleanup):
    cursor.execute(f'drop table if exists {table_name}')
    cursor.execute(f'create table {table_name}(id int primary key, col1 varchar)')
    cursor.execute(f"select col1, id from {table_name}")
    end = cursor.fetchone()
    assert end is None


def test_fetchone_table_many_rows(table_name, cursor, drop_table_cleanup):
    create_and_populate_test_table(cursor, TEST_ROWS_NUM, table_name)

    cursor.execute(f"select id, data, fl from {table_name} order by id")

    for i in range(TEST_ROWS_NUM):
        row = cursor.fetchone()
        check_row(i, row)

    end = cursor.fetchone()
    assert end is None


def test_fetchmany_table_empty(table_name, cursor, drop_table_cleanup):
    cursor.execute(f'drop table if exists {table_name}')
    cursor.execute(f'create table {table_name}(id int primary key, col1 varchar)')
    cursor.execute(f"select col1, id from {table_name}")
    end = cursor.fetchmany(size=10)
    assert end is not None
    assert len(end) == 0


def test_fetchmany_table_many_rows(table_name, cursor, drop_table_cleanup):
    create_and_populate_test_table(cursor, TEST_ROWS_NUM, table_name)

    cursor.arraysize = 5
    cursor.execute(f"select id, data, fl from {table_name} order by id")

    rows0_4 = cursor.fetchmany()
    assert len(rows0_4) == 5
    for i in range(5):
        check_row(i, rows0_4[i])

    rows5_12 = cursor.fetchmany(size=8)
    assert len(rows5_12) == 8
    for i in range(8):
        check_row(i + 5, rows5_12[i])

    rows13_14 = cursor.fetchmany()
    assert len(rows13_14) == 2
    for i in range(2):
        check_row(i + 13, rows13_14[i])

    end = cursor.fetchone()
    assert end is None


def test_fetchall_table_empty(table_name, cursor, drop_table_cleanup):
    cursor.execute(f'drop table if exists {table_name}')
    cursor.execute(f'create table {table_name}(id int primary key, col1 varchar)')
    cursor.execute(f"select col1, id from {table_name}")
    end = cursor.fetchall()
    assert end is not None
    assert len(end) == 0


def test_fetchall_table_many_rows(table_name, cursor, drop_table_cleanup):
    create_and_populate_test_table(cursor, TEST_ROWS_NUM, table_name)

    cursor.arraysize = 5
    cursor.execute(f"select id, data, fl from {table_name} order by id")

    rows_all = cursor.fetchall()
    assert len(rows_all) == TEST_ROWS_NUM
    for i in range(TEST_ROWS_NUM):
        check_row(i, rows_all[i])

    end = cursor.fetchone()
    assert end is None


def test_fetch_mixed_table_many_rows(table_name, cursor, drop_table_cleanup):
    create_and_populate_test_table(cursor, TEST_ROWS_NUM, table_name)

    cursor.arraysize = 4
    cursor.execute(f"select id, data, fl from {table_name} order by id")

    assert cursor.rownumber == 0

    rows0_3 = cursor.fetchmany()
    assert len(rows0_3) == 4
    for i in range(4):
        check_row(i, rows0_3[i])

    assert cursor.rownumber == 4

    row4 = cursor.fetchone()
    check_row(4, row4)

    assert cursor.rownumber == 5

    rows_remaining = cursor.fetchall()
    assert len(rows_remaining) == TEST_ROWS_NUM - 5
    for i in range(TEST_ROWS_NUM - 5):
        check_row(i + 5, rows_remaining[i])

    assert cursor.rownumber is None

    end = cursor.fetchone()
    assert end is None


def test_fetchone_table_many_rows_parameter(table_name, cursor, drop_table_cleanup):
    create_and_populate_test_table(cursor, TEST_ROWS_NUM, table_name)

    cursor.execute(f"select id, data, fl from {table_name} where id = ? order by id", [13])

    row = cursor.fetchone()
    check_row(13, row)

    end = cursor.fetchone()
    assert end is None


def test_insert_arguments_fetchone(table_name, cursor, drop_table_cleanup):
    cursor.execute(f'create table {table_name}(id int primary key, data varchar, fl double)')
    for i in range(TEST_ROWS_NUM):
        cursor.execute(f"insert into {table_name} values (?, ?, ?)", [i, f'Value-{i * 2}', i / 2.0])

    cursor.execute(f"select id, data, fl from {table_name} where id = ?", [3])

    row = cursor.fetchone()
    check_row(3, row)

    end = cursor.fetchone()
    assert end is None


def test_cursor_iterable(table_name, cursor, drop_table_cleanup):
    create_and_populate_test_table(cursor, TEST_ROWS_NUM, table_name)

    cursor.execute(f"select id, data, fl from {table_name} order by id")

    for i, row in enumerate(cursor):
        check_row(i, row)


@pytest.mark.parametrize("rows_num", [
    TEST_PAGE_SIZE - 1,
    TEST_PAGE_SIZE,
    TEST_PAGE_SIZE + 1,
    TEST_PAGE_SIZE * 2,
    TEST_PAGE_SIZE * 2 + 1,
    10000,
])
def test_fetch_table_several_pages(table_name, cursor, drop_table_cleanup, rows_num):
    create_and_populate_test_table(cursor, rows_num, table_name, 1000)

    cursor.execute(f"select id, data, fl from {table_name} order by id")

    rows_all = cursor.fetchall()
    assert len(rows_all) == rows_num
    for i in range(rows_num):
        check_row(i, rows_all[i])

    end = cursor.fetchone()
    assert end is None
