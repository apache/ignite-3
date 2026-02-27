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

import pyignite_dbapi


def test_non_query(table_name, cursor):
    with pytest.raises(pyignite_dbapi.ProgrammingError):
        cursor.execute("Non query")


def test_unknown_table(table_name, cursor):
    with pytest.raises(pyignite_dbapi.ProgrammingError):
        cursor.execute(f"select * from {table_name}")


def test_unknown_column(table_name, cursor, drop_table_cleanup):
    cursor.execute(f'create table {table_name}(id int primary key, data varchar)')
    with pytest.raises(pyignite_dbapi.ProgrammingError):
        cursor.execute(f"select unknown_col from {table_name}")


def test_unknown_schema(table_name, cursor):
    with pytest.raises(pyignite_dbapi.ProgrammingError):
        cursor.execute(f'create table UNKNOWN_SCHEMA.{table_name}(id int primary key, data varchar)')


def test_table_exists(table_name, cursor, drop_table_cleanup):
    cursor.execute(f'create table {table_name}(id int primary key, data varchar)')
    with pytest.raises(pyignite_dbapi.ProgrammingError):
        cursor.execute(f'create table {table_name}(id int primary key, data varchar)')


def test_column_exists(table_name, cursor, drop_table_cleanup):
    cursor.execute(f'create table {table_name}(id int primary key, data varchar)')
    with pytest.raises(pyignite_dbapi.ProgrammingError):
        cursor.execute(f'alter table {table_name} add data varchar')


def test_cursor_state_fetch(table_name, cursor):
    with pytest.raises(pyignite_dbapi.InterfaceError):
        cursor.fetchone()


def test_cursor_state_proc(table_name, cursor):
    with pytest.raises(pyignite_dbapi.NotSupportedError):
        cursor.callproc()


def test_arithmetic_div_by_zero(table_name, cursor):
    with pytest.raises(pyignite_dbapi.DatabaseError):
        cursor.execute('select 1 / 0')


def test_column_constraints_size(table_name, cursor, drop_table_cleanup):
    cursor.execute(f'create table {table_name}(id int primary key, data varchar(5))')
    with pytest.raises(pyignite_dbapi.ProgrammingError):
        cursor.execute(f"insert into {table_name} values (1, '1234567890')")


def test_column_constraints_nulls(table_name, cursor, drop_table_cleanup):
    cursor.execute(f'create table {table_name}(id int primary key, data varchar not null)')
    with pytest.raises(pyignite_dbapi.IntegrityError):
        cursor.execute(f"insert into {table_name} values (1, NULL)")

