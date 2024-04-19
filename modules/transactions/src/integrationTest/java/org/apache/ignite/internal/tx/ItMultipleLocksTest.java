/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.tx;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.sql.IgniteSql;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/**
 * Test producing massive number of locks which can cause race conditions on lock manager.
 */
public class ItMultipleLocksTest extends ClusterPerTestIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    @RepeatedTest(100)
    void test() {
        var node0 = runningNodes().findAny().orElseThrow();

        IgniteSql sql = node0.sql();
        sql.execute(null, "CREATE TABLE test("
                + "c1 INT PRIMARY KEY, c2 INT, c3 INT, c4 INT, c5 INT,"
                + "c6 INT, c7 INT, c8 INT, c9 INT, c10 INT)");

        for (int i = 2; i <= 10; i++) {
            sql.execute(null, format("CREATE INDEX c{}_idx ON test (c{})", i, i));
        }

        sql.execute(null, "INSERT INTO test"
                + " SELECT x as c1, x as c2, x as c3, x as c4, x as c5, "
                + "        x as c6, x as c7, x as c8, x as c9, x as c10"
                + "   FROM TABLE (system_range(1, 20000))");
    }
}
