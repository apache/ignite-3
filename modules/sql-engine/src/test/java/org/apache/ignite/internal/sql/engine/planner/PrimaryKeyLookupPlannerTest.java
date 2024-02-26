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
import static org.hamcrest.Matchers.nullValue;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.commands.DropTableCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.prepare.KeyValueGetPlan;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test cases to very KV lookup optimized plans.
 */
public class PrimaryKeyLookupPlannerTest extends AbstractPlannerTest {
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
        for (CatalogTableDescriptor table : CLUSTER.catalogManager().tables(version)) {
            commands.add(
                    DropTableCommand.builder()
                            .schemaName(CatalogService.DEFAULT_SCHEMA_NAME)
                            .tableName(table.name())
                            .build()
            );
        }

        await(CLUSTER.catalogManager().execute(commands));
    }

    @Test
    void optimizedGetUsedForSingleOptionConditions() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        {
            QueryPlan plan = node.prepare("SELECT * FROM test WHERE id = 1");

            assertThat(plan, instanceOf(KeyValueGetPlan.class));
            assertKeyExpressions((KeyValueGetPlan) plan, "1");
            assertEmptyCondition((KeyValueGetPlan) plan);
        }

        {
            QueryPlan plan = node.prepare("SELECT * FROM test WHERE id IN(1)");

            assertThat(plan, instanceOf(KeyValueGetPlan.class));
            assertKeyExpressions((KeyValueGetPlan) plan, "1");
            assertEmptyCondition((KeyValueGetPlan) plan);
        }
    }

    @Test
    void optimizedGetUsedForConditionWithPostFiltration() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("SELECT * FROM test WHERE id = 1 AND val > 10");

        assertThat(plan, instanceOf(KeyValueGetPlan.class));

        assertKeyExpressions((KeyValueGetPlan) plan, "1");
        assertCondition((KeyValueGetPlan) plan, ">($t1, 10)");
    }

    @Test
    void optimizedGetUsedWithProjections() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("SELECT id, 2 * val AS new_val FROM test WHERE id = 1");

        assertThat(plan, instanceOf(KeyValueGetPlan.class));

        assertKeyExpressions((KeyValueGetPlan) plan, "1");
        assertProjection((KeyValueGetPlan) plan, "$t0", "*(2, $t1)");
        assertEmptyCondition((KeyValueGetPlan) plan);
    }

    @Test
    void optimizedGetNotUsedForRange() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("SELECT * FROM test WHERE id > 1");

        assertThat(plan, not(instanceOf(KeyValueGetPlan.class)));
    }

    @Test
    void optimizedGetNotUsedForMultiBounds() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        {
            QueryPlan plan = node.prepare("SELECT * FROM test WHERE id = 1 OR id = 2");

            assertThat(plan, not(instanceOf(KeyValueGetPlan.class)));
        }

        {
            QueryPlan plan = node.prepare("SELECT * FROM test WHERE id IN (1, 2)");

            assertThat(plan, not(instanceOf(KeyValueGetPlan.class)));
        }
    }

    @Test
    void optimizedGetNotUsedForPartiallyCoveredKey() {
        node.initSchema("CREATE TABLE test (id1 INT, id2 INT, val INT, PRIMARY KEY (id1, id2))");

        {
            QueryPlan plan = node.prepare("SELECT * FROM test WHERE id1 = 1");

            assertThat(plan, not(instanceOf(KeyValueGetPlan.class)));
        }

        {
            QueryPlan plan = node.prepare("SELECT id1 FROM test WHERE id1 = 1");

            assertThat(plan, not(instanceOf(KeyValueGetPlan.class)));
        }
    }

    @Test
    void optimizedGetUsedWithComplexKeysNormalOrder() {
        node.initSchema("CREATE TABLE test (id1 INT, id2 INT, val INT, PRIMARY KEY(id1, id2))");

        QueryPlan plan = node.prepare("SELECT * FROM test WHERE id1 = 1 AND id2 IN (2)");

        assertThat(plan, instanceOf(KeyValueGetPlan.class));
        assertKeyExpressions((KeyValueGetPlan) plan, "1", "2");
        assertEmptyCondition((KeyValueGetPlan) plan);
    }

    @Test
    void optimizedGetUsedWithComplexKeysReversOrder() {
        node.initSchema("CREATE TABLE test (id1 INT, id2 INT, val INT, PRIMARY KEY(id2, id1))");

        QueryPlan plan = node.prepare("SELECT * FROM test WHERE id1 = 1 AND id2 IN (2)");

        assertThat(plan, instanceOf(KeyValueGetPlan.class));
        assertKeyExpressions((KeyValueGetPlan) plan, "2", "1");
        assertEmptyCondition((KeyValueGetPlan) plan);
    }

    private static void assertKeyExpressions(KeyValueGetPlan plan, String... expectedExpressions) {
        List<String> keyExpressions = plan.lookupNode().keyExpressions().stream()
                .map(RexNode::toString)
                .collect(toList());

        assertThat(
                keyExpressions,
                equalTo(List.of(expectedExpressions))
        );
    }

    private static void assertCondition(KeyValueGetPlan plan, String expectedCondition) {
        assertThat(
                plan.lookupNode().condition().toString(),
                equalTo(expectedCondition)
        );
    }

    private static void assertEmptyCondition(KeyValueGetPlan plan) {
        assertThat(
                plan.lookupNode().condition(),
                nullValue()
        );
    }

    private static void assertProjection(KeyValueGetPlan plan, String... expectedProjections) {
        List<String> projections = plan.lookupNode().projects().stream()
                .map(RexNode::toString)
                .collect(toList());

        assertThat(
                projections,
                equalTo(List.of(expectedProjections))
        );
    }
}
