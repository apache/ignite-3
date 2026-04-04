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


# TODO: IGNITE-26358: Enable heartbeats in tests and add variant with batch_size 300 and 2000 once heartbeats are implemented
@pytest.mark.parametrize("batch_size", [1, 2, 10])
def test_executemany_success(table_name, cursor, drop_table_cleanup, batch_size):
    test_data = [(i, f'data_{i}') for i in range(batch_size)]

    cursor.execute(f'create table {table_name}(id int primary key, data varchar)')
    cursor.executemany(f"insert into {table_name} values(?, ?)", test_data)
    cursor.execute(f"select id, data from {table_name} order by id")

    for i in range(batch_size):
        row = cursor.fetchone()
        row_expected = test_data[i]
        assert len(row) == len(row_expected)
        assert row == row_expected

    end = cursor.fetchone()
    assert end is None
