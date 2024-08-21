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
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.testframework.SystemPropertiesExtension;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for SELECT COUNT(*) optimization.
 */
@ExtendWith(SystemPropertiesExtension.class)
public class ItSqlUsesSelectCountOptimizedTest extends BaseSqlIntegrationTest {

    @BeforeAll
    @SuppressWarnings("ConcatenationWithEmptyString")
    static void initSchema() {
        CLUSTER.aliveNode().sql().executeScript(""
                + "CREATE TABLE test (id INT PRIMARY KEY, val INT);"
                + "INSERT INTO test SELECT x, x FROM TABLE(system_range(1, 10));");
    }

    @Test
    public void countOpt() {
        assertQuery("SELECT COUNT(*) FROM test")
                .matches(QueryChecker.containsSubPlan("SelectCount"))
                .returns(10L)
                .check();

        assertQuery("SELECT 1, COUNT(*) FROM test")
                .matches(QueryChecker.containsSubPlan("SelectCount"))
                .returns(1, 10L)
                .check();

        assertQuery("SELECT ?, COUNT(*) FROM test")
                .withParam(1)
                .matches(QueryChecker.containsSubPlan("SelectCount"))
                .returns(1, 10L)
                .check();

        assertQuery("SELECT COUNT(1) FROM test")
                .matches(QueryChecker.containsSubPlan("SelectCount"))
                .returns(10L)
                .check();

        assertQuery("SELECT COUNT(1), 1, COUNT(*) FROM test")
                .matches(QueryChecker.containsSubPlan("SelectCount"))
                .returns(10L, 1, 10L)
                .check();

        assertQuery("SELECT COUNT(*) FROM test as x (a, b)")
                .matches(QueryChecker.containsSubPlan("SelectCount"))
                .returns(10L)
                .check();

        assertQuery("SELECT COUNT(a) FROM test as x (a, b)")
                .matches(QueryChecker.containsSubPlan("SelectCount"))
                .returns(10L)
                .check();
    }

    @Test
    // TODO: https://issues.apache.org/jira/browse/IGNITE-22821 replace with feature toggle
    @WithSystemProperty(key = "FAST_QUERY_OPTIMIZATION_ENABLED", value = "false")
    public void optimizationDisabled() {
        assertQuery("SELECT COUNT(*) FROM test")
                .matches(QueryChecker.containsSubPlan("Aggregate"))
                .returns(10L)
                .check();
    }

    @Test
    public void notOptimizeAliasedWhenUsesNull() {
        assertQuery("SELECT COUNT(b) FROM test as x (a, b)")
                .matches(QueryChecker.containsSubPlan("Aggregate"))
                .returns(10L)
                .check();
    }

    @Test
    public void noOptimizationForRoTx() {
        Transaction tx = igniteTx().begin(new TransactionOptions().readOnly(true));

        assertQuery((InternalTransaction) tx, "SELECT COUNT(*) FROM test")
                .matches(QueryChecker.containsSubPlan("Aggregate"))
                .returns(10L)
                .check();

        tx.commit();
    }

    @Test
    public void noOptimizationForRwTx() {
        Transaction tx = igniteTx().begin(new TransactionOptions().readOnly(false));

        assertQuery((InternalTransaction) tx, "SELECT COUNT(*) FROM test")
                .matches(QueryChecker.containsSubPlan("Aggregate"))
                .returns(10L)
                .check();

        tx.commit();
    }

    @Test
    public void noOptimizationForSystemView() {
        assertQuery("SELECT COUNT(*) FROM SYSTEM.SYSTEM_VIEWS")
                .matches(QueryChecker.containsSubPlan("Aggregate"))
                .returnRowCount(1)
                .check();
    }
}
