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
from tests.util import server_addresses_invalid, server_addresses_basic


def test_connection_success():
    conn = pyignite3.connect(address=server_addresses_basic, timeout=1)
    assert conn is not None
    conn.close()


def test_connection_get_cursor():
    with pyignite3.connect(address=server_addresses_basic, timeout=1) as conn:
        assert conn is not None

        cursor = conn.cursor()
        assert cursor.connection is conn
        cursor.close()


def test_connection_fail():
    with pytest.raises(pyignite3.OperationalError) as err:
        pyignite3.connect(address=server_addresses_invalid, timeout=1)
    assert err.match("Failed to establish connection with the host.")
