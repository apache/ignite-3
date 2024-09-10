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

import pyignite3
import pytest

from tests.util import check_cluster_started, start_cluster_gen, server_addresses_basic

logger = logging.getLogger('pyignite3')
logger.setLevel(logging.DEBUG)


@pytest.fixture()
def table_name(request):
    return request.node.originalname


@pytest.fixture()
def connection():
    conn = pyignite3.connect(address=server_addresses_basic[0])
    yield conn
    conn.close()


@pytest.fixture()
def cursor(connection):
    cursor = connection.cursor()
    yield cursor
    cursor.close()


@pytest.fixture(autouse=True, scope="session")
def cluster():
    if not check_cluster_started():
        yield from start_cluster_gen()
    else:
        yield None

