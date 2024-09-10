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
import pyignite3
from tests.util import server_addresses_basic


def test_fetch_constant_string():
    with pyignite3.connect(address=server_addresses_basic[0]) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select 'Lorem ipsum'")
            data = cursor.fetchone()
            assert len(data) == 1
            assert data[0] == 'Lorem ipsum'
