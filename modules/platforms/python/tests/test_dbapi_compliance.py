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
import dbapi20

from tests.util import server_addresses_basic


class TestPyignite3(dbapi20.DatabaseAPI20Test):
    driver = pyignite3
    connect_args = ()
    connect_kw_args = {'address': server_addresses_basic}
    lower_func = 'lower'

    prefix = dbapi20.DatabaseAPI20Test.table_prefix
    ddl1 = 'create table %sbooze (name varchar(20) primary key)' % prefix
    ddl2 = 'create table %sbarflys (name varchar(20) primary key, drink varchar(30))' % prefix

    def setUp(self):
        dbapi20.DatabaseAPI20Test.setUp(self)
        #
        # try:
        #     con = self._connect()
        #     con.close()
        # except:
        #     cmd = [ "psql", "-c", "create database dbapi20_test" ]
        #     if subprocess.call(cmd):
        #         self.fail("Failed to create databse.")

    def tearDown(self):
        dbapi20.DatabaseAPI20Test.tearDown(self)

    def test_non_idempotent_close(self): pass
    def test_nextset(self): pass
    def test_setoutputsize(self): pass

