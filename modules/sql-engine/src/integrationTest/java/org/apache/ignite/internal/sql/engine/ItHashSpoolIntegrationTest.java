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

package org.apache.ignite.internal.sql.engine;

import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Hash spool test.
 */
public class ItHashSpoolIntegrationTest extends BaseSqlIntegrationTest {
    @Test
    public void testNullsInSearchRow() {
        sql("CREATE TABLE t(pk varchar default gen_random_uuid PRIMARY KEY, i1 INTEGER, i2 INTEGER)");
        sql("INSERT INTO t (i1, i2) VALUES (null, 0), (1, 1), (2, 2), (3, null)");

        assertQuery("SELECT i1, (SELECT i2 FROM t WHERE i1=t1.i1) FROM t t1")
                .returns(null, null)
                .returns(1, 1)
                .returns(2, 2)
                .returns(3, null)
                .check();

        assertQuery("SELECT (SELECT i1 FROM t WHERE i2=t1.i2), i2 FROM t t1")
                .returns(null, 0)
                .returns(1, 1)
                .returns(2, 2)
                .returns(null, null)
                .check();
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-21286")
    public void testNullsInSearchRowMultipleColumns() {
        sql("CREATE TABLE t0(pk varchar default gen_random_uuid PRIMARY KEY, i1 INTEGER, i2 INTEGER)");
        sql("CREATE TABLE t1(pk varchar default gen_random_uuid PRIMARY KEY, i1 INTEGER, i2 INTEGER)");
        sql("INSERT INTO t0(i1, i2) VALUES (null, 0), (1, null), (null, 2), (3, null), (1, 1)");
        sql("INSERT INTO t1(i1, i2) VALUES (null, 0), (null, 1), (2, null), (3, null), (1, 1)");

        String sql = "SELECT t0.i1, t0.i2, t1.i1, t1.i2 "
                + "FROM t0 JOIN t1 ON t0.i1=t1.i1 AND t0.i2=t1.i2";

        assertQuery(sql)
                .disableRules("MergeJoinConverter", "NestedLoopJoinConverter", "FilterSpoolMergeToSortedIndexSpoolRule")
                .returns(1, 1, 1, 1)
                .check();
    }
}
