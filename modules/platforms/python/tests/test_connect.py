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
from tests.util import server_addresses_invalid, server_addresses_basic


@pytest.mark.parametrize('address', [server_addresses_basic, server_addresses_basic[0]])
def test_connection_success(address):
    conn = pyignite_dbapi.connect(address=address, timeout=1)
    assert conn is not None
    conn.close()


@pytest.mark.parametrize('address', [server_addresses_basic, server_addresses_basic[0]])
def test_connection_get_cursor(address):
    with pyignite_dbapi.connect(address=address, timeout=1) as conn:
        assert conn is not None

        cursor = conn.cursor()
        assert cursor.connection is conn
        cursor.close()


@pytest.mark.parametrize('address', [server_addresses_invalid, server_addresses_invalid[0]])
def test_connection_fail(address):
    with pytest.raises(pyignite_dbapi.OperationalError) as err:
        pyignite_dbapi.connect(address=address, timeout=1)
    assert err.match('Failed to establish connection with the cluster.')


ERR_MSG_WRONG_TYPE = "Only a string or a list of strings are allowed in 'address' parameter"
ERR_MSG_EMPTY = "No addresses provided to connect"

@pytest.mark.parametrize('address,err_msg', [
    (123, ERR_MSG_WRONG_TYPE),
    ([123], ERR_MSG_WRONG_TYPE),
    ([server_addresses_basic[0], 123], ERR_MSG_WRONG_TYPE),
    ([], ERR_MSG_EMPTY),
    ('', ERR_MSG_EMPTY),
    ([''], ERR_MSG_EMPTY),
])
def test_connection_wrong_arg(address, err_msg):
    with pytest.raises(pyignite_dbapi.InterfaceError) as err:
        pyignite_dbapi.connect(address=address, timeout=1)
    assert err.match(err_msg)
