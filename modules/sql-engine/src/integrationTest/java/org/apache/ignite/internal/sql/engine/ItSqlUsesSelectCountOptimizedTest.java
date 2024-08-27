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
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for SELECT COUNT(*) optimization.
 */
public class ItSqlUsesSelectCountOptimizedTest extends BaseSqlIntegrationTest {

    @BeforeAll
    @SuppressWarnings("ConcatenationWithEmptyString")
    static void initSchema() {
        CLUSTER.aliveNode().sql().executeScript(""
                + "CREATE TABLE test (id INT PRIMARY KEY, val INT);"
                + "INSERT INTO test SELECT x, x FROM TABLE(system_range(1, 10));");
    }

    @BeforeEach
    @AfterEach
    public void resetFastOpt() {
        System.setProperty("FAST_QUERY_OPTIMIZATION_ENABLED", String.valueOf(Commons.fastQueryOptimizationEnabled()));
    }

    @Test
    public void countOpt() {
        {
            Transaction tx = igniteTx().begin(new TransactionOptions().readOnly(false));

            assertQuery((InternalTransaction) tx, "SELECT COUNT(*) FROM test")
                    .matches(QueryChecker.containsSubPlan("Aggregate"))
                    .returns(10L)
                    .columnNames("COUNT(*)")
                    .check();

            tx.commit();
        }

        {
            Transaction tx = igniteTx().begin(new TransactionOptions().readOnly(true));

            assertQuery((InternalTransaction) tx, "SELECT COUNT(*) FROM test")
                    .matches(QueryChecker.containsSubPlan("Aggregate"))
                    .returns(10L)
                    .columnNames("COUNT(*)")
                    .check();

            tx.commit();
        }

        assertQuery("SELECT COUNT(*) FROM test")
                .matches(QueryChecker.containsSubPlan("SelectCount"))
                .returns(10L)
                .columnNames("COUNT(*)")
                .check();

        assertQuery("SELECT 1, COUNT(*) FROM test")
                .matches(QueryChecker.containsSubPlan("SelectCount"))
                .returns(1, 10L)
                .columnNames("1", "COUNT(*)")
                .check();

        assertQuery("SELECT ?, COUNT(*) FROM test")
                .withParam(1)
                .matches(QueryChecker.containsSubPlan("SelectCount"))
                .returns(1, 10L)
                .columnNames("EXPR$0", "COUNT(*)")
                .check();

        assertQuery("SELECT COUNT(1) FROM test")
                .matches(QueryChecker.containsSubPlan("SelectCount"))
                .returns(10L)
                .columnNames("COUNT(1)")
                .check();

        assertQuery("SELECT COUNT(1), 1, COUNT(*) FROM test")
                .matches(QueryChecker.containsSubPlan("SelectCount"))
                .returns(10L, 1, 10L)
                .columnNames("COUNT(1)", "1", "COUNT(*)")
                .check();

        assertQuery("SELECT COUNT(*) FROM test as x (a, b)")
                .matches(QueryChecker.containsSubPlan("SelectCount"))
                .returns(10L)
                .columnNames("COUNT(*)")
                .check();

        assertQuery("SELECT COUNT(id) FROM test")
                .matches(QueryChecker.containsSubPlan("Aggregate"))
                .returns(10L)
                .columnNames("COUNT(ID)")
                .check();

        // Disable fast query optimization
        // TODO: https://issues.apache.org/jira/browse/IGNITE-22821 replace with feature toggle
        System.setProperty("FAST_QUERY_OPTIMIZATION_ENABLED", "false");

        assertQuery("SELECT COUNT(*) FROM test")
                .matches(QueryChecker.containsSubPlan("Aggregate"))
                .returns(10L)
                .columnNames("COUNT(*)")
                .check();

        assertQuery("SELECT COUNT(id) FROM test")
                .matches(QueryChecker.containsSubPlan("Aggregate"))
                .returns(10L)
                .columnNames("COUNT(ID)")
                .check();

        assertQuery("SELECT COUNT(b) FROM test as x (a, b)")
                .matches(QueryChecker.containsSubPlan("Aggregate"))
                .returns(10L)
                .columnNames("COUNT(B)")
                .check();
    }

    @Test
    public void noOptimizationForSystemView() {
        assertQuery("SELECT COUNT(*) FROM SYSTEM.SYSTEM_VIEWS")
                .matches(QueryChecker.containsSubPlan("Aggregate"))
                .returnRowCount(1)
                .check();
    }
}
