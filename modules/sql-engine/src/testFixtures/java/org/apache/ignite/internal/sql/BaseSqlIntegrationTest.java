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

package org.apache.ignite.internal.sql;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.util.InjectQueryCheckerFactory;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.sql.engine.util.QueryCheckerExtension;
import org.apache.ignite.internal.sql.engine.util.QueryCheckerFactory;
import org.apache.ignite.internal.systemview.SystemViewManagerImpl;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.Table;
import org.apache.ignite.tx.IgniteTransactions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Base class for SQL integration tests.
 */
@ExtendWith(QueryCheckerExtension.class)
public class BaseSqlIntegrationTest extends ClusterPerClassIntegrationTest {
    @InjectQueryCheckerFactory
    protected static QueryCheckerFactory queryCheckerFactory;

    /**
     * Executes the query and validates any asserts passed to the builder.
     *
     * @param qry Query to execute.
     * @return Instance of QueryChecker.
     */
    protected static QueryChecker assertQuery(String qry) {
        return assertQuery((InternalTransaction) null, qry);
    }

    protected static QueryChecker assertQuery(IgniteImpl node, String qry) {
        return assertQuery(node, null, qry);
    }

    /**
     * Executes the query with the given transaction and validates any asserts passed to the builder.
     *
     * @param tx Transaction.
     * @param qry Query to execute.
     * @return Instance of QueryChecker.
     */
    protected static QueryChecker assertQuery(@Nullable InternalTransaction tx, String qry) {
        return assertQuery(CLUSTER.aliveNode(), tx, qry);
    }

    protected static QueryChecker assertQuery(IgniteImpl node, @Nullable InternalTransaction tx, String qry) {
        return queryCheckerFactory.create(node.name(), node.queryEngine(), node.transactions(), tx, qry);
    }

    /**
     * Used for join checks, disables other join rules for executing exact join algo.
     *
     * @param qry Query for check.
     * @param joinType Type of join algo.
     * @param rules Additional rules need to be disabled.
     */
    protected static QueryChecker assertQuery(String qry, JoinType joinType, String... rules) {
        return assertQuery(qry)
                .disableRules(joinType.disabledRules)
                .disableRules(rules);
    }

    /**
     * Used for query with aggregates checks, disables other aggregate rules for executing exact agregate algo.
     *
     * @param qry Query for check.
     * @param aggregateType Type of aggregate algo.
     * @param rules Additional rules need to be disabled.
     */
    protected static QueryChecker assertQuery(String qry, AggregateType aggregateType, String... rules) {
        return assertQuery(qry)
                .disableRules(aggregateType.disabledRules)
                .disableRules(rules);
    }

    /**
     * Join type.
     */
    protected enum JoinType {
        NESTED_LOOP(
                "CorrelatedNestedLoopJoin",
                "JoinCommuteRule",
                "MergeJoinConverter",
                "HashJoinConverter"
        ),

        MERGE(
                "CorrelatedNestedLoopJoin",
                "JoinCommuteRule",
                "NestedLoopJoinConverter",
                "HashJoinConverter"
        ),

        CORRELATED(
                "MergeJoinConverter",
                "JoinCommuteRule",
                "NestedLoopJoinConverter",
                "HashJoinConverter"
        ),

        HASHJOIN(
                "MergeJoinConverter",
                "JoinCommuteRule",
                "NestedLoopJoinConverter",
                "CorrelatedNestedLoopJoin"
        );

        private final String[] disabledRules;

        JoinType(String... disabledRules) {
            this.disabledRules = disabledRules;
        }
    }

    /**
     * Aggregate type.
     */
    protected enum AggregateType {
        SORT(
                "ColocatedHashAggregateConverterRule",
                "ColocatedSortAggregateConverterRule",
                "MapReduceHashAggregateConverterRule"
        ),

        HASH(
                "ColocatedHashAggregateConverterRule",
                "ColocatedSortAggregateConverterRule",
                "MapReduceSortAggregateConverterRule"
        );

        private final String[] disabledRules;

        AggregateType(String... disabledRules) {
            this.disabledRules = disabledRules;
        }
    }

    protected static void createAndPopulateTable() {
        createTable(DEFAULT_TABLE_NAME, 1, 8);

        int idx = 0;

        insertData("person", List.of("ID", "NAME", "SALARY"), new Object[][]{
                {idx++, "Igor", 10d},
                {idx++, null, 15d},
                {idx++, "Ilya", 15d},
                {idx++, "Roma", 10d},
                {idx, "Roma", 10d}
        });
    }

    protected static void checkMetadata(ColumnMetadata expectedMeta, ColumnMetadata actualMeta) {
        assertAll("Missmatch:\n expected = " + expectedMeta + ",\n actual = " + actualMeta,
                () -> assertEquals(expectedMeta.name(), actualMeta.name(), "name"),
                () -> assertEquals(expectedMeta.nullable(), actualMeta.nullable(), "nullable"),
                () -> assertSame(expectedMeta.type(), actualMeta.type(), "type"),
                () -> assertEquals(expectedMeta.precision(), actualMeta.precision(), "precision"),
                () -> assertEquals(expectedMeta.scale(), actualMeta.scale(), "scale"),
                () -> assertSame(expectedMeta.valueClass(), actualMeta.valueClass(), "value class"),
                () -> {
                    if (expectedMeta.origin() == null) {
                        assertNull(actualMeta.origin(), "origin");

                        return;
                    }

                    assertNotNull(actualMeta.origin(), "origin");
                    assertEquals(expectedMeta.origin().schemaName(), actualMeta.origin().schemaName(), " origin schema");
                    assertEquals(expectedMeta.origin().tableName(), actualMeta.origin().tableName(), " origin table");
                    assertEquals(expectedMeta.origin().columnName(), actualMeta.origin().columnName(), " origin column");
                }
        );
    }

    /**
     * Returns transaction manager for first cluster node.
     */
    protected IgniteTransactions igniteTx() {
        return CLUSTER.aliveNode().transactions();
    }

    /**
     * Gets the SQL API.
     *
     * @return SQL API.
     */
    protected IgniteSql igniteSql() {
        return CLUSTER.aliveNode().sql();
    }

    /**
     * Returns internal  {@code SqlQueryProcessor} for first cluster node.
     */
    protected SqlQueryProcessor queryProcessor() {
        return (SqlQueryProcessor) CLUSTER.aliveNode().queryEngine();
    }

    /**
     * Returns internal {@code TxManager} for first cluster node.
     */
    protected TxManager txManager() {
        return CLUSTER.aliveNode().txManager();
    }

    protected static Table table(String canonicalName) {
        return CLUSTER.aliveNode().tables().table(canonicalName);
    }

    /**
     * Returns internal {@code SystemViewManager} for first cluster node.
     */
    protected SystemViewManagerImpl systemViewManager() {
        return (SystemViewManagerImpl) CLUSTER.aliveNode().systemViewManager();
    }
}
