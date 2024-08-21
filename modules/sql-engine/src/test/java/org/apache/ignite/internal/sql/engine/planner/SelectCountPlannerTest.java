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
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.commands.DropTableCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.sql.engine.framework.ExplicitTxContext;
import org.apache.ignite.internal.sql.engine.framework.ImplicitTxContext;
import org.apache.ignite.internal.sql.engine.framework.NoOpTransaction;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.prepare.ExplainPlan;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.prepare.SelectCountPlan;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.testframework.SystemPropertiesExtension;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests to verify SELECT COUNT(*) optimized plans.
 */
@ExtendWith(SystemPropertiesExtension.class)
public class SelectCountPlannerTest extends AbstractPlannerTest {

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
                            .schemaName(SqlCommon.DEFAULT_SCHEMA_NAME)
                            .tableName(table.name())
                            .build()
            );
        }

        await(CLUSTER.catalogManager().execute(commands));
    }

    @Test
    public void optimizeCountStar() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("SELECT count(*) FROM test");

        assertThat(plan, instanceOf(SelectCountPlan.class));
        assertExpressions((SelectCountPlan) plan, "$0");
    }

    @Test
    public void optimizeCountLiteral() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("SELECT count(1) FROM test");

        assertThat(plan, instanceOf(SelectCountPlan.class));
        assertExpressions((SelectCountPlan) plan, "$0");
    }

    @Test
    public void optimizeCountWithSimpleProjection() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("SELECT 1, count(1), 42 FROM test");

        assertThat(plan, instanceOf(SelectCountPlan.class));

        SelectCountPlan countPlan = (SelectCountPlan) plan;
        assertExpressions(countPlan, "1", "$0", "42");
    }

    @Test
    public void optimizeCountWithDynamicParamsProjection() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("SELECT ?, count(1), 2 FROM test", 1);

        assertThat(plan, instanceOf(SelectCountPlan.class));

        SelectCountPlan countPlan = (SelectCountPlan) plan;
        assertExpressions(countPlan, "?0", "$0", "2");
    }

    @Test
    public void optimizeCountNonNullableCol() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("SELECT count(id) FROM test");

        assertThat(plan, instanceOf(SelectCountPlan.class));
        assertExpressions((SelectCountPlan) plan, "$0");
    }

    @Test
    public void optimizeCountNonNulleCols() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT NOT NULL)");

        QueryPlan plan = node.prepare("SELECT count(id), count(val) FROM test");

        assertThat(plan, instanceOf(SelectCountPlan.class));
        assertExpressions((SelectCountPlan) plan, "$0", "$0");
    }

    @Test
    // TODO: https://issues.apache.org/jira/browse/IGNITE-22821 replace with feature toggle
    @WithSystemProperty(key = "FAST_QUERY_OPTIMIZATION_ENABLED", value = "true")
    public void optimizeCountStarWhenEnabled() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("SELECT count(*) FROM test");

        assertThat(plan, instanceOf(SelectCountPlan.class));
    }

    @Test
    public void optimizeCountStarWithOrderBy() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        {
            QueryPlan plan = node.prepare("SELECT count(*) FROM test ORDER BY 1");

            assertThat(plan, instanceOf(SelectCountPlan.class));
            assertExpressions((SelectCountPlan) plan, "$0");
        }

        {
            QueryPlan plan = node.prepare("SELECT count(*) FROM test ORDER BY 1");

            assertThat(plan, instanceOf(SelectCountPlan.class));
            assertExpressions((SelectCountPlan) plan, "$0");
        }

        {
            QueryPlan plan = node.prepare("SELECT count(*) FROM test ORDER BY 1 OFFSET 2");
            assertThat(plan, not(instanceOf(SelectCountPlan.class)));
        }

        {
            QueryPlan plan = node.prepare("SELECT count(*), 1 as c FROM test ORDER BY c");
            assertThat(plan, not(instanceOf(SelectCountPlan.class)));
        }
    }

    @Test
    public void optimizeAliased() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("SELECT count(*) FROM test AS x(a, b)");

        assertThat(plan, instanceOf(SelectCountPlan.class));
    }

    @Test
    // TODO: https://issues.apache.org/jira/browse/IGNITE-22821 replace with feature toggle
    @WithSystemProperty(key = "FAST_QUERY_OPTIMIZATION_ENABLED", value = "false")
    public void doNotOptimizedIfDisabled() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("SELECT count(*) FROM test");

        assertThat(plan, not(instanceOf(SelectCountPlan.class)));
    }

    @Test
    public void doNotOptimizeCountNullableCol() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("SELECT count(val) FROM test");

        assertThat(plan, not(instanceOf(SelectCountPlan.class)));
    }

    @Test
    public void doNotOptimizeCountFunc() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("SELECT count(CURRENT_TIME) FROM test");

        assertThat(plan, not(instanceOf(SelectCountPlan.class)));
    }

    @Test
    public void doNotOptimizeCountWithComplexCountArgs() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("SELECT count(1+1) FROM test");

        assertThat(plan, not(instanceOf(SelectCountPlan.class)));
    }

    @Test
    public void doNotOptimizeCountWithComplexProjection() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("SELECT 1, count(1), 42+1 FROM test");

        assertThat(plan, not(instanceOf(SelectCountPlan.class)));
    }

    @Test
    public void doNotOptimizeCountWithDynamicParamArgs() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("SELECT count(?) FROM test", 1);

        assertThat(plan, not(instanceOf(SelectCountPlan.class)));
    }

    @Test
    public void doNotOptimizeCountWithWhereClause() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("SELECT count(id) FROM test WHERE id > 0");

        assertThat(plan, not(instanceOf(SelectCountPlan.class)));
    }

    @Test
    public void doNotOptimizeCountWithGroupByClause() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("SELECT count(*) FROM test GROUP by id");

        assertThat(plan, not(instanceOf(SelectCountPlan.class)));
    }

    @Test
    public void doNotOptimizeCountWithHavingClause() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("SELECT count(*) FROM test HAVING count(id) > 0");

        assertThat(plan, not(instanceOf(SelectCountPlan.class)));
    }

    @Test
    public void doNotOptimizeCountWithLimit() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("SELECT count(*) FROM test LIMIT 3");

        assertThat(plan, not(instanceOf(SelectCountPlan.class)));
    }

    @Test
    public void doNotOptimizeCountWithOffset() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("SELECT count(*) FROM test OFFSET 3");

        assertThat(plan, not(instanceOf(SelectCountPlan.class)));
    }

    @Test
    public void doNotOptimizeCountWithFetch() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("SELECT count(*) FROM test FETCH FIRST 3 ROWS ONLY");

        assertThat(plan, not(instanceOf(SelectCountPlan.class)));
    }

    @Test
    public void doNotOptimizeCountValues() {
        QueryPlan plan = node.prepare("SELECT count(c) FROM (VALUES (1), (2), (4)) t(c)");

        assertThat(plan, not(instanceOf(SelectCountPlan.class)));
    }

    @Test
    public void doNotOptimizeCountInsideSelect() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("SELECT * FROM (SELECT count(*) as c FROM test) t(c)");

        assertThat(plan, not(instanceOf(SelectCountPlan.class)));
    }

    @Test
    public void doNotOptimizeCountDistinct() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        QueryPlan plan = node.prepare("SELECT count(DISTINCT(id)) FROM test");

        assertThat(plan, not(instanceOf(SelectCountPlan.class)));
    }

    @Test
    public void explainSelectCount() {
        node.initSchema("CREATE TABLE test (id INT PRIMARY KEY, val INT)");

        {
            ExplainPlan plan = (ExplainPlan) node.prepare("EXPLAIN PLAN FOR SELECT count(*) FROM test");
            assertThat(plan.plan().explain(), containsString("SelectCount"));
        }

        {
            QueryTransactionContext txContext = ImplicitTxContext.INSTANCE;

            ExplainPlan plan = (ExplainPlan) node.prepare("EXPLAIN PLAN FOR SELECT count(*) FROM test", txContext);
            assertThat(plan.plan().explain(), containsString("SelectCount"));
        }

        {
            NoOpTransaction tx = NoOpTransaction.readWrite("RW");
            QueryTransactionContext txContext = ExplicitTxContext.fromTx(tx);

            ExplainPlan plan = (ExplainPlan) node.prepare("EXPLAIN PLAN FOR SELECT count(*) FROM test", txContext);
            assertThat(plan.plan().explain(), not(containsString("SelectCount")));
        }

        {
            NoOpTransaction tx = NoOpTransaction.readOnly("RO");
            QueryTransactionContext txContext = ExplicitTxContext.fromTx(tx);

            ExplainPlan plan = (ExplainPlan) node.prepare("EXPLAIN PLAN FOR SELECT count(*) FROM test", txContext);
            assertThat(plan.plan().explain(), not(containsString("SelectCount")));
        }
    }

    private static void assertExpressions(SelectCountPlan plan, String... expectedExpressions) {
        List<String> expressions = plan.selectCountNode().expressions().stream()
                .map(RexNode::toString)
                .collect(toList());

        assertThat(
                expressions,
                equalTo(List.of(expectedExpressions))
        );

        RelDataType rowType = plan.selectCountNode().getRowType();
        assertEquals(expectedExpressions.length, rowType.getFieldCount(), "output columns");
    }
}
