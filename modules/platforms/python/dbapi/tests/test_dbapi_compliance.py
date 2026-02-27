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
import pyignite_dbapi
import dbapi20

from tests.util import server_addresses_basic, check_cluster_started, start_cluster, kill_process_tree


class TestPyIgniteDbApi(dbapi20.DatabaseAPI20Test):
    driver = pyignite_dbapi
    connect_args = ()
    connect_kw_args = {'address': server_addresses_basic}
    lower_func = 'lower'

    prefix = dbapi20.DatabaseAPI20Test.table_prefix
    ddl1 = 'create table %sbooze (name varchar(20) primary key)' % prefix
    ddl2 = 'create table %sbarflys (name varchar(20) primary key, drink varchar(30))' % prefix

    def setUp(self):
        dbapi20.DatabaseAPI20Test.setUp(self)
        self._srv = None
        if not check_cluster_started():
            self._srv = start_cluster()

    def tearDown(self):
        dbapi20.DatabaseAPI20Test.tearDown(self)
        if self._srv:
            kill_process_tree(self._srv.pid)

    def test_callproc(self):
        # Stored procedures are not supported
        pass

    def test_executemany(self):
        pass

    def test_nextset(self):
        # TODO: IGNITE-22743 Implement execution of SQL scripts
        pass

    def test_non_idempotent_close(self):
        # There is no use in raising error on double close.
        pass

    def test_setinputsizes(self):
        # setoutputsize does not do anything currently.
        pass

    def test_setoutputsize(self):
        # setoutputsize does not do anything currently.
        pass
