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
from tests.util import server_addresses_basic

test_data = [
    0,
    1,
    -1,
    2,
    43,
    -543656,
    423538409739,
    0.0,
    123.456,
    -76.4,
    1.0E-40,
    1.0E40,
    'test',
    'TEST',
    'Lorem Ipsum',
    '你好',
    'Мир!',
    '',
    True,
    False,
    None,
    b'',
    b'0',
    b'123456789',
    b'h9832y9r8wf08hw85h0h2508h0858',
    b'\x45\xf0\xab',
]


def check_fetch_parameters(cursor, param, use_tuple: bool):
    cursor.execute("select ?", (param,) if use_tuple else [param])
    data = cursor.fetchone()
    assert len(data) == 1
    if isinstance(param, float):
        assert data[0] == pytest.approx(param)
    else:
        assert data[0] == param


@pytest.mark.parametrize("param", test_data)
def test_fetch_parameter_list(cursor, param):
    check_fetch_parameters(cursor, param, False)


@pytest.mark.parametrize("param", test_data)
def test_fetch_parameter_tuple(cursor, param):
    check_fetch_parameters(cursor, param, True)
