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

/**
 * Test cases to very KV delete optimized plans.
 */
public class PrimaryKeyDeletePlannerTest extends AbstractPlannerTest {
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
        List<CatalogCommand> commands = new ArrayList<>();
        for (CatalogTableDescriptor table : CLUSTER.catalogManager().latestCatalog().tables()) {
            commands.add(
                    DropTableCommand.builder()
                            .schemaName(SqlCommon.DEFAULT_SCHEMA_NAME)
                            .tableName(table.name())
                            .build()
            );
        }

        await(CLUSTER.catalogManager().execute(commands));
    }

    @Test
    void optimizedDeleteUsedForSingleOptionConditions() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        {
            QueryPlan plan = node.prepare("DELETE FROM test WHERE id = 1");

            assertThat(plan, instanceOf(KeyValueModifyPlan.class));
            assertThat(((KeyValueModifyPlan) plan).getRel().operation(), equalTo(Operation.DELETE));
            assertKeyExpressions((KeyValueModifyPlan) plan, "1");
        }

        {
            QueryPlan plan = node.prepare("DELETE FROM test WHERE id IN(1)");

            assertThat(plan, instanceOf(KeyValueModifyPlan.class));
            assertThat(((KeyValueModifyPlan) plan).getRel().operation(), equalTo(Operation.DELETE));
            assertKeyExpressions((KeyValueModifyPlan) plan, "1");
        }
    }

    @Test
    void optimizedDeleteNotUsedForConditionWithPostFiltration() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("DELETE FROM test WHERE id = 1 AND val > 10");

        assertThat(plan, not(instanceOf(KeyValueModifyPlan.class)));
    }

    @Test
    void optimizedDeleteNotUsedForRange() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("DELETE FROM test WHERE id > 1");

        assertThat(plan, not(instanceOf(KeyValueModifyPlan.class)));
    }

    @Test
    void optimizedDeleteNotUsedForMultiBounds() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        {
            QueryPlan plan = node.prepare("DELETE FROM test WHERE id = 1 OR id = 2");

            assertThat(plan, not(instanceOf(KeyValueModifyPlan.class)));
        }

        {
            QueryPlan plan = node.prepare("DELETE FROM test WHERE id IN (1, 2)");

            assertThat(plan, not(instanceOf(KeyValueModifyPlan.class)));
        }
    }

    @Test
    void optimizedDeleteNotUsedForPartiallyCoveredKey() {
        node.initSchema("CREATE TABLE test (id1 INT, id2 INT, val INT, PRIMARY KEY (id1, id2))");

        {
            QueryPlan plan = node.prepare("DELETE FROM test WHERE id1 = 1");

            assertThat(plan, not(instanceOf(KeyValueModifyPlan.class)));
        }
    }

    @Test
    void optimizedDeleteUsedWithComplexKeysNormalOrder() {
        node.initSchema("CREATE TABLE test (id1 INT, id2 INT, val INT, PRIMARY KEY(id1, id2))");

        QueryPlan plan = node.prepare("DELETE FROM test WHERE id1 = 1 AND id2 IN (2)");

        assertThat(plan, instanceOf(KeyValueModifyPlan.class));
        assertThat(((KeyValueModifyPlan) plan).getRel().operation(), equalTo(Operation.DELETE));
        assertKeyExpressions((KeyValueModifyPlan) plan, "1", "2");
    }

    @Test
    void optimizedDeleteUsedWithComplexKeysReversOrder() {
        node.initSchema("CREATE TABLE test (id1 INT, id2 INT, val INT, PRIMARY KEY(id2, id1))");

        QueryPlan plan = node.prepare("DELETE FROM test WHERE id1 = 1 AND id2 IN (2)");

        assertThat(plan, instanceOf(KeyValueModifyPlan.class));
        assertThat(((KeyValueModifyPlan) plan).getRel().operation(), equalTo(Operation.DELETE));
        assertKeyExpressions((KeyValueModifyPlan) plan, "2", "1");
    }

    @Test
    void optimizedDeleteWithOutOfRangeKey() {
        node.initSchema("CREATE TABLE test (id TINYINT PRIMARY KEY, val INT)");

        // TODO: https://issues.apache.org/jira/browse/IGNITE-26158
        // // Out of range: TINYINT_MAX + 1.
        // {
        //     QueryPlan plan = node.prepare("DELETE FROM test WHERE id = 128");
        // 
        //     assertThat(plan, instanceOf(MultiStepPlan.class));
        //     assertEmptyValuesNode((MultiStepPlan) plan);
        // }
        // 
        // // Out of range: TINYINT_MIN - 1.
        // {
        //     QueryPlan plan = node.prepare("DELETE FROM test WHERE id = -129");
        // 
        //     assertThat(plan, instanceOf(MultiStepPlan.class));
        //     assertEmptyValuesNode((MultiStepPlan) plan);
        // }

        // TINYINT_MAX
        {
            QueryPlan plan = node.prepare("DELETE FROM test WHERE id = 127");

            assertThat(plan, instanceOf(KeyValueModifyPlan.class));
            assertThat(((KeyValueModifyPlan) plan).getRel().operation(), equalTo(Operation.DELETE));
            assertKeyExpressions((KeyValueModifyPlan) plan, "127:TINYINT");
        }

        // TINYINT_MIN
        {
            QueryPlan plan = node.prepare("DELETE FROM test WHERE id = -128");

            assertThat(plan, instanceOf(KeyValueModifyPlan.class));
            assertThat(((KeyValueModifyPlan) plan).getRel().operation(), equalTo(Operation.DELETE));
            assertKeyExpressions((KeyValueModifyPlan) plan, "-128:TINYINT");
        }
    }

    private static void assertKeyExpressions(KeyValueModifyPlan plan, String... expectedExpressions) {
        List<String> keyExpressions = (plan.getRel()).expressions().stream()
                .map(RexNode::toString)
                .collect(toList());

        assertThat(
                keyExpressions,
                equalTo(List.of(expectedExpressions))
        );
    }
}
