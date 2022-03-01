/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsSubPlan;

import org.junit.jupiter.api.Test;

/** Tests for correlated queries. */
public class ItCorrelatesTest extends AbstractBasicIntegrationTest {
    private static final String DISABLED_JOIN_RULES = " /*+ DISABLE_RULE('MergeJoinConverter', 'NestedLoopJoinConverter') */ ";

    /** Checks correlates are assigned before access. */
    @Test
    public void testCorrelatesAssignedBeforeAccess() {
        sql("create table test_tbl(k INTEGER primary key, v INTEGER)");
        sql("INSERT INTO test_tbl VALUES (1, 1)");

        assertQuery("SELECT " + DISABLED_JOIN_RULES + " t0.v, (SELECT t0.v + t1.v FROM test_tbl t1) AS j FROM test_tbl t0")
                .matches(containsSubPlan("IgniteCorrelatedNestedLoopJoin"))
                .returns(1, 2)
                .check();
    }

    /** Checks that correlates can't be moved under the table spool. */
    @Test
    public void testCorrelatesWithTableSpool() {
        sql("CREATE TABLE test(k INTEGER primary key, i1 INT, i2 INT)");
        sql("INSERT INTO test VALUES (1, 1, 1), (2, 2, 2)");

        assertQuery("SELECT " + DISABLED_JOIN_RULES + " (SELECT t1.i1 + t1.i2 + t0.i2 FROM test t1 WHERE i1 = 1) FROM test t0")
                .matches(containsSubPlan("IgniteCorrelatedNestedLoopJoin"))
                .matches(containsSubPlan("IgniteTableSpool"))
                .returns(3)
                .returns(4)
                .check();
    }
}
