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
import logging

import pyignite_dbapi
import pytest

from tests.util import check_cluster_started, start_cluster_gen, server_addresses_basic

logger = logging.getLogger('pyignite_dbapi')
logger.setLevel(logging.DEBUG)

TEST_PAGE_SIZE = 32

@pytest.fixture()
def table_name(request):
    return request.node.originalname


@pytest.fixture()
def connection():
    conn = pyignite_dbapi.connect(address=server_addresses_basic, page_size=TEST_PAGE_SIZE, heartbeat_interval=2)
    yield conn
    conn.close()

@pytest.fixture()
def service_connection():
    conn = pyignite_dbapi.connect(address=server_addresses_basic, page_size=TEST_PAGE_SIZE, heartbeat_interval=2)
    yield conn
    conn.close()


@pytest.fixture()
def cursor(connection):
    cursor = connection.cursor()
    yield cursor
    cursor.close()


@pytest.fixture()
def service_cursor(service_connection):
    cursor = service_connection.cursor()
    yield cursor
    cursor.close()


@pytest.fixture()
def drop_table_cleanup(service_cursor, table_name):
    service_cursor.execute(f'drop table if exists {table_name}')
    yield None
    service_cursor.execute(f'drop table if exists {table_name}')


@pytest.fixture(autouse=True, scope="session")
def cluster():
    if not check_cluster_started():
        yield from start_cluster_gen()
    else:
        yield None

