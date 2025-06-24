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
import os
from typing import Optional

import pytest

import pyignite_dbapi
from tests.util import server_addresses_basic, server_addresses_ssl_basic, server_addresses_ssl_client_auth, get_test_dir


def create_ssl_param(use_ssl : bool, ssl_key : Optional[str], ssl_cert : Optional[str], ssl_ca : Optional[str]):
    ssl_dir_path = os.path.join(get_test_dir(), 'ssl')

    return {
        'use_ssl': use_ssl,
        'ssl_keyfile': os.path.join(ssl_dir_path, ssl_key) if ssl_key else '',
        'ssl_certfile': os.path.join(ssl_dir_path, ssl_cert) if ssl_cert else '',
        'ssl_ca_certfile': os.path.join(ssl_dir_path, ssl_ca) if ssl_ca else '',
    }


def test_connection_success():
    ssl_cfg = create_ssl_param(True, 'client.pem', 'client.pem', 'ca.pem')
    conn = pyignite_dbapi.connect(address=server_addresses_ssl_basic, timeout=1, **ssl_cfg)
    assert conn is not None
    conn.close()


def test_connection_unknown():
    ssl_cfg = create_ssl_param(True, 'client_unknown.pem', 'client_unknown.pem', 'ca.pem')
    conn = pyignite_dbapi.connect(address=server_addresses_ssl_basic, timeout=1, **ssl_cfg)
    assert conn is not None
    conn.close()


def test_connection_unknown_2():
    with pytest.raises(pyignite_dbapi.OperationalError) as err:
        ssl_cfg = create_ssl_param(True, 'client_unknown.pem', 'client_unknown.pem', 'ca.pem')
        pyignite_dbapi.connect(address=server_addresses_ssl_client_auth, timeout=1, **ssl_cfg)
    assert err.match('Failed to (send|get) handshake (request|response)')


def test_connection_reject():
    with pytest.raises(pyignite_dbapi.OperationalError) as err:
        ssl_cfg = create_ssl_param(False, 'client.pem', 'client.pem', 'ca.pem')
        pyignite_dbapi.connect(address=server_addresses_ssl_basic, timeout=1, **ssl_cfg)
    assert err.match('Failed to (send|get) handshake (request|response)')


def test_connection_reject_2():
    with pytest.raises(pyignite_dbapi.OperationalError) as err:
        ssl_cfg = create_ssl_param(True, 'client.pem', 'client.pem', 'ca.pem')
        pyignite_dbapi.connect(address=server_addresses_basic, timeout=1, **ssl_cfg)
    assert err.match('Can not establish secure connection')


def test_connection_no_certs():
    with pytest.raises(pyignite_dbapi.OperationalError) as err:
        ssl_cfg = create_ssl_param(True, None, None, 'ca.pem')
        pyignite_dbapi.connect(address=server_addresses_ssl_client_auth, timeout=1, **ssl_cfg)
    assert err.match('Failed to (send|get) handshake (request|response)')


def test_connection_non_existing_ca():
    with pytest.raises(pyignite_dbapi.OperationalError) as err:
        ssl_cfg = create_ssl_param(True, 'client.pem', 'client.pem', 'non_existing_ca.pem')
        pyignite_dbapi.connect(address=server_addresses_ssl_client_auth, timeout=1, **ssl_cfg)
    assert err.match('Can not set Certificate Authority path for secure connection')


def test_connection_non_existing_key():
    with pytest.raises(pyignite_dbapi.OperationalError) as err:
        ssl_cfg = create_ssl_param(True, 'non_existing_key.pem', 'client.pem', 'ca.pem')
        pyignite_dbapi.connect(address=server_addresses_ssl_client_auth, timeout=1, **ssl_cfg)
    assert err.match('Can not set private key file for secure connection')
    assert err.match('(No such file or directory)|(no such file)')


def test_connection_non_existing_cert():
    with pytest.raises(pyignite_dbapi.OperationalError) as err:
        ssl_cfg = create_ssl_param(True, 'client.pem', 'non_existing_key.pem', 'ca.pem')
        pyignite_dbapi.connect(address=server_addresses_ssl_client_auth, timeout=1, **ssl_cfg)
    assert err.match('Can not set client certificate file for secure connection')
    assert err.match('(No such file or directory)|(no such file)')


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


@pytest.mark.parametrize("address", [server_addresses_ssl_basic, server_addresses_ssl_client_auth])
def test_fetch_table_several_pages(table_name, address, drop_table_cleanup):
    ssl_cfg = create_ssl_param(True, 'client.pem', 'client.pem', 'ca.pem')
    with pyignite_dbapi.connect(address=address, timeout=1, **ssl_cfg) as connection:
        with connection.cursor() as cursor:
            rows_num = 345
            create_and_populate_test_table(cursor, rows_num, table_name, 1000)

            cursor.execute(f"select id, data, fl from {table_name} order by id")

            rows_all = cursor.fetchall()
            assert len(rows_all) == rows_num
            for i in range(rows_num):
                check_row(i, rows_all[i])

            end = cursor.fetchone()
            assert end is None
