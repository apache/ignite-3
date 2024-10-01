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

import pyignite3


@pytest.fixture()
def create_tx_test_table(cursor, table_name):
    cursor.execute(f'drop table if exists {table_name}')
    cursor.execute(f'create table {table_name}(id int primary key, val int)')


def test_commit_rollback(table_name, connection, cursor, create_tx_test_table, drop_table_cleanup):
    connection.setautocommit(False)

    cursor.execute(f'insert into {table_name} values (42, 10)')
    connection.commit()

    cursor.execute(f'update {table_name} set val=23 where id=42')
    connection.rollback()

    cursor.execute(f'select val from {table_name} where id=42')
    row = cursor.fetchone()

    assert row[0] == 10

