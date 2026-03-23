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

import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsSubPlan;

import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Tests for correlated queries. */
public class ItCorrelatesTest extends BaseSqlIntegrationTest {
    private static final String DISABLED_JOIN_RULES = " /*+ DISABLE_RULE('MergeJoinConverter', 'NestedLoopJoinConverter', "
            + "'HashJoinConverter'), disable_decorrelation */ ";

    @AfterEach
    public void dropTables() {
        dropAllTables();
    }

    /** Checks correlates are assigned before access. */
    @Test
    public void testCorrelatesAssignedBeforeAccess() {
        sql("create table test_tbl(k INTEGER primary key, v INTEGER)");

        sql("INSERT INTO test_tbl VALUES (1, 1)");

        assertQuery("SELECT " + DISABLED_JOIN_RULES + " t0.v, (SELECT t0.v + t1.v FROM test_tbl t1) AS j FROM test_tbl t0")
                .matches(containsSubPlan("CorrelatedNestedLoopJoin"))
                .returns(1, 2)
                .check();
    }

    @Test
    void testProcessCorrelatesFromLeft() {
        //noinspection ConcatenationWithEmptyString
        sqlScript(""
                + "CREATE TABLE t0 ("
                + " id INT PRIMARY KEY,"
                + " padding_col1 INT,"
                + " padding_col2 INT,"
                + " val INT);"
                + "CREATE TABLE t1 (id INT PRIMARY KEY, val INT);");

        sql("insert into t0 values(1, 1, 1, 10), (2, 2, 2, 20);");
        sql("insert into t1 values(1, 10), (2, 20);");

        assertQuery("SELECT /*+ disable_decorrelation */ t1.ID FROM t0 JOIN t1 ON "
                + "(t1.id = (SELECT inner_t1.id FROM t1 AS inner_t1 WHERE inner_t1.val = t0.val)) ORDER BY 1")
                .matches(containsSubPlan("CorrelatedNestedLoopJoin"))
                .returns(1)
                .returns(2)
                .check();
    }

    @Test
    void testProcessCorrelatesFromRight() {
        //noinspection ConcatenationWithEmptyString
        sqlScript(""
                + "CREATE TABLE t0 ("
                + " id INT PRIMARY KEY,"
                + " val INT);"
                + "CREATE TABLE t1 (id INT PRIMARY KEY, val INT);");

        sql("insert into t0 values(1, 10), (2, 20);");
        sql("insert into t1 values(1, 10), (2, 20);");

        assertQuery("SELECT /*+ disable_decorrelation */ t1.ID FROM t0 JOIN t1 ON "
                + "(t0.id = (SELECT inner_t0.id FROM t0 AS inner_t0 WHERE inner_t0.val = t1.val)) ORDER BY 1")
                .matches(containsSubPlan("CorrelatedNestedLoopJoin"))
                .returns(1)
                .returns(2)
                .check();
    }

    /**
     * Tests resolving of collisions in correlates with correlate variables in the left hand.
     */
    @Test
    public void testCorrelatesCollisionLeft() {
        sql("CREATE TABLE test1 (a INTEGER PRIMARY KEY, b INTEGER)");
        sql("CREATE TABLE test2 (a INTEGER PRIMARY KEY, c INTEGER)");

        sql("INSERT INTO test1 VALUES (11, 1), (12, 2), (13, 3)");
        sql("INSERT INTO test2 VALUES (11, 1), (12, 1), (13, 4)");

        // Collision by correlate variables in the left hand.
        assertQuery("SELECT /*+ disable_decorrelation */ * FROM test1 WHERE "
                + "EXISTS(SELECT * FROM test2 WHERE test1.a=test2.a AND test1.b<>test2.c) "
                + "AND NOT EXISTS(SELECT * FROM test2 WHERE test1.a=test2.a AND test1.b<test2.c)")
                .returns(12, 2)
                .check();
    }

    /**
     * Tests resolving of collisions in correlates with correlate variables in both, left and right hands.
     */
    @Test
    public void testCorrelatesCollisionRight() {
        sql("CREATE TABLE test1 (a INTEGER PRIMARY KEY, b INTEGER)");
        sql("CREATE TABLE test2 (a INTEGER PRIMARY KEY, c INTEGER)");

        sql("INSERT INTO test1 VALUES (11, 1), (12, 2), (13, 3)");
        sql("INSERT INTO test2 VALUES (11, 1), (12, 1), (13, 4)");

        // Collision by correlate variables in both, left and right hands.
        assertQuery("SELECT /*+ disable_decorrelation */ * FROM test1 WHERE "
                + "EXISTS(SELECT * FROM test2 WHERE (SELECT test1.a)=test2.a AND (SELECT test1.b)<>test2.c) "
                + "AND NOT EXISTS(SELECT * FROM test2 WHERE (SELECT test1.a)=test2.a AND (SELECT test1.b)<test2.c)")
                .returns(12, 2)
                .check();
    }

    @Test
    public void testCorrelations() {
        sql("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)");
        sql("CREATE TABLE t2 (id INTEGER PRIMARY KEY, val INTEGER)");
        sql("CREATE TABLE t3 (id INTEGER PRIMARY KEY, val INTEGER)");

        sql("INSERT INTO t1 VALUES(1, 2)");
        sql("INSERT INTO t1 VALUES(13, 14)");
        sql("INSERT INTO t1 VALUES(42, 43)");

        sql("INSERT INTO t2 VALUES(1, 2)");
        sql("INSERT INTO t2 VALUES(42, 43)");

        sql("INSERT INTO t3 VALUES(1, 11)");
        sql("INSERT INTO t3 VALUES(13, 23)");
        sql("INSERT INTO t3 VALUES(42, 52)");

        // t1 -> t2 (t2 references t1)

        assertQuery("SELECT /*+ disable_decorrelation */ * FROM t1 as cor WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = cor.id)")
                .returns(1, 2)
                .returns(42, 43)
                .check();

        assertQuery("SELECT /*+ disable_decorrelation */ * FROM t1 as cor WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.id = cor.id)")
                .returns(13, 14)
                .check();

        // t3 -> t1 -> t2 (t2 references t1)

        assertQuery("SELECT /*+ disable_decorrelation */ * FROM t3 AS out\n"
                + "WHERE EXISTS (SELECT * FROM t1 as cor WHERE out.id = cor.id AND EXISTS "
                + "(SELECT 1 FROM t2 WHERE t2.id = cor.id))")
                .returns(1, 11)
                .returns(42, 52)
                .check();

        assertQuery("SELECT /*+ disable_decorrelation */ * FROM t3 AS out\n"
                + "WHERE NOT EXISTS (SELECT * FROM t1 as cor WHERE out.id = cor.id AND EXISTS "
                + "(SELECT 1 FROM t2 WHERE t2.id = cor.id))")
                .returns(13, 23)
                .check();

        // t3 -> t1 -> t2 (t2 references both t3 and t1)

        assertQuery("SELECT /*+ disable_decorrelation */ * FROM t3 AS out\n"
                + "WHERE EXISTS (SELECT * FROM t1 as cor WHERE out.id = cor.id AND EXISTS "
                + "(SELECT 1 FROM t2 WHERE t2.id = out.id OR t2.id = cor.id))")
                .returns(1, 11)
                .returns(42, 52)
                .check();
    }
}
