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

package org.apache.ignite.internal.sql.engine.planner;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.commands.DropTableCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.prepare.KeyValueModifyPlan;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueModify.Operation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test cases to very KV modify optimized plans.
 */
public class KeyValueModifyPlannerTest extends AbstractPlannerTest {
    private static final String NODE_NAME = "N1";

    private static final TestCluster CLUSTER = TestBuilders.cluster()
            .nodes(NODE_NAME)
            .build();

    private final TestNode node = CLUSTER.node(NODE_NAME);

    @BeforeAll
    static void start() {
        CLUSTER.start();
    }

    @AfterAll
    static void stop() throws Exception {
        CLUSTER.stop();
    }

    @AfterEach
    void clearCatalog() {
        int version = CLUSTER.catalogManager().latestCatalogVersion();

        List<CatalogCommand> commands = new ArrayList<>();
        for (CatalogTableDescriptor table : CLUSTER.catalogManager().catalog(version).tables()) {
            commands.add(
                    DropTableCommand.builder()
                            .schemaName(SqlCommon.DEFAULT_SCHEMA_NAME)
                            .tableName(table.name())
                            .build()
            );
        }

        await(CLUSTER.catalogManager().execute(commands));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "INSERT INTO test VALUES (10, 20)",
            "INSERT INTO test(id, val) VALUES (10, 20)",
            "INSERT INTO test(val, id) VALUES (20, 10)",
    })
    void optimizedInsertUsedForLiterals(String insertStatement) {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare(insertStatement);

        assertThat(plan, instanceOf(KeyValueModifyPlan.class));
        assertThat(((KeyValueModifyPlan) plan).getRel().operation(), equalTo(Operation.INSERT));
        assertExpressions((KeyValueModifyPlan) plan, "10", "20");
    }

    @Test
    void optimizedInsertUsedForDynamicParams() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        {
            QueryPlan plan = node.prepare("INSERT INTO test VALUES (?, ?)");

            assertThat(plan, instanceOf(KeyValueModifyPlan.class));
            assertThat(((KeyValueModifyPlan) plan).getRel().operation(), equalTo(Operation.INSERT));
            assertExpressions((KeyValueModifyPlan) plan, "?0", "?1");
        }

        {
            QueryPlan plan = node.prepare("INSERT INTO test(id, val) VALUES (?, ?)");

            assertThat(plan, instanceOf(KeyValueModifyPlan.class));
            assertThat(((KeyValueModifyPlan) plan).getRel().operation(), equalTo(Operation.INSERT));
            assertExpressions((KeyValueModifyPlan) plan, "?0", "?1");
        }

        {
            QueryPlan plan = node.prepare("INSERT INTO test(val, id) VALUES (?, ?)");

            assertThat(plan, instanceOf(KeyValueModifyPlan.class));
            assertThat(((KeyValueModifyPlan) plan).getRel().operation(), equalTo(Operation.INSERT));
            assertExpressions((KeyValueModifyPlan) plan, "?1", "?0");
        }
    }

    @Test
    void optimizedInsertUsedForMixedCase() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, int_val INT, str_val VARCHAR(128))");

        QueryPlan plan = node.prepare("INSERT INTO test VALUES (?, 1, CAST(CURRENT_DATE as VARCHAR(128)))");

        assertThat(plan, instanceOf(KeyValueModifyPlan.class));
        assertThat(((KeyValueModifyPlan) plan).getRel().operation(), equalTo(Operation.INSERT));
        assertExpressions((KeyValueModifyPlan) plan, "?0", "1", "CAST(CURRENT_DATE):VARCHAR(128) CHARACTER SET \"UTF-8\" NOT NULL");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "INSERT INTO test(id) VALUES (1)",
            "INSERT INTO test(id, int_val) VALUES (1, DEFAULT)"
    })
    void optimizedInsertUsedWithDefaults(String insertStatement) {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, int_val INT DEFAULT 10, str_val VARCHAR(128) DEFAULT 'a')");

        QueryPlan plan = node.prepare(insertStatement);

        assertThat(plan, instanceOf(KeyValueModifyPlan.class));
        assertThat(((KeyValueModifyPlan) plan).getRel().operation(), equalTo(Operation.INSERT));
        assertExpressions((KeyValueModifyPlan) plan, "1", "10", "_UTF-8'a'");
    }

    @Test
    void optimizedInsertNotUsedForMultiInsert() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("INSERT INTO test VALUES (1, 1), (2, 2)");

        assertThat(plan, not(instanceOf(KeyValueModifyPlan.class)));
    }

    @Test
    void optimizedInsertNotUsedForInsertFromOtherTable() {
        node.initSchema("CREATE TABLE source_t (id INT PRIMARY KEY, val INT);"
                + "CREATE TABLE target_t (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("INSERT INTO target_t SELECT * FROM source_t");

        assertThat(plan, not(instanceOf(KeyValueModifyPlan.class)));
    }

    private static void assertExpressions(KeyValueModifyPlan plan, String... expectedExpressions) {
        List<String> keyExpressions = (plan.getRel()).expressions().stream()
                .map(RexNode::toString)
                .collect(toList());

        assertThat(
                keyExpressions,
                equalTo(List.of(expectedExpressions))
        );
    }
}
