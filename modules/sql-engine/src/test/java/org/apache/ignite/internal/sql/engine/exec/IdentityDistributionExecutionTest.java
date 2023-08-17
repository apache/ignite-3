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

package org.apache.ignite.internal.sql.engine.exec;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.sql.engine.AsyncCursor;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.ClusterBuilder;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders.ClusterTableBuilder;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.CursorUtils;
import org.junit.jupiter.api.Test;

/**
 * Execution test for {@link org.apache.ignite.internal.sql.engine.trait.DistributionFunction.IdentityDistribution} function.
 */
public class IdentityDistributionExecutionTest extends AbstractPlannerTest {

    private static final String NODE1_NAME = "node1";
    private static final String NODE2_NAME = "node2";
    private static final String NODE3_NAME = "node3";

    @Test
    public void joinTablesWithSameDistribution() {
        TestCluster cluster = createClusterWith(identityTable("TEST_TBL1"), identityTable("TEST_TBL2"));

        cluster.start();

        String sql = "select * "
                + "from TEST_TBL1 t1 "
                + "left join TEST_TBL2 t2 on t1.id = t2.id";

        TestNode node = cluster.node(NODE1_NAME);

        QueryPlan queryPlan = node.prepare(sql);
        AsyncCursor<List<Object>> cursor = node.executePlan(queryPlan);

        List<List<Object>> allFromCursor = CursorUtils.getAllFromCursor(cursor);
        assertThat(allFromCursor.size(), is(3 * 25));
    }

    @Test
    public void joinAffinityToIdentity() {
        TestCluster cluster = createClusterWith(affinityTable("TEST_TBL1"), identityTable("TEST_TBL2"));

        cluster.start();

        String sql = "select * "
                + "from TEST_TBL1 t1 "
                + "left join TEST_TBL2 t2 on t1.id = t2.id";

        TestNode node = cluster.node(NODE1_NAME);

        QueryPlan queryPlan = node.prepare(sql);
        AsyncCursor<List<Object>> cursor = node.executePlan(queryPlan);

        List<List<Object>> allFromCursor = CursorUtils.getAllFromCursor(cursor);
        assertThat(allFromCursor.size(), is(7 + 3 * 5));
    }

    @Test
    public void joinIdetityToAffinity() {
        TestCluster cluster = createClusterWith(identityTable("TEST_TBL1"), affinityTable("TEST_TBL2"));

        cluster.start();

        String sql = "select * "
                + "from TEST_TBL1 t1 "
                + "left join TEST_TBL2 t2 on t1.id = t2.id";

        TestNode node = cluster.node(NODE1_NAME);

        QueryPlan queryPlan = node.prepare(sql);
        AsyncCursor<List<Object>> cursor = node.executePlan(queryPlan);

        List<List<Object>> allFromCursor = CursorUtils.getAllFromCursor(cursor);
        assertThat(allFromCursor.size(), is(15));
    }

    private static UnaryOperator<ClusterTableBuilder> identityTable(String tableName) {
        return builder -> builder.name(tableName)
                .addColumn("ID", NativeTypes.STRING)
                .addColumn("VAL", NativeTypes.STRING)
                .distribution(IgniteDistributions.identity(0))
                .addDataProvider(NODE1_NAME, identityTableProvider(NODE1_NAME))
                .addDataProvider(NODE2_NAME, identityTableProvider(NODE2_NAME))
                .addDataProvider(NODE3_NAME, identityTableProvider(NODE3_NAME));
    }

    private static UnaryOperator<ClusterTableBuilder> affinityTable(String tableName) {
        Function<Integer, List<Object[]>> rowGenerator = nodeId -> IntStream.range(0, 10)
                .filter(i -> 0 == (i + nodeId) % 3)
                .mapToObj(i -> new Object[]{"node" + i, String.valueOf(i + 100)}).collect(Collectors.toList());

        return builder -> builder.name(tableName)
                .addColumn("ID", NativeTypes.STRING)
                .addColumn("VAL", NativeTypes.STRING)
                .distribution(IgniteDistributions.affinity(0, nextTableId(), 0))
                .addDataProvider(NODE1_NAME, DataProvider.fromCollection(rowGenerator.apply(1)))
                .addDataProvider(NODE2_NAME, DataProvider.fromCollection(rowGenerator.apply(2)))
                .addDataProvider(NODE3_NAME, DataProvider.fromCollection(rowGenerator.apply(3)));
    }

    private static TestCluster createClusterWith(UnaryOperator<ClusterTableBuilder>... builders) {
        ClusterBuilder clusterBuilder = TestBuilders.cluster().nodes(NODE1_NAME, NODE2_NAME, NODE3_NAME);

        for (UnaryOperator<ClusterTableBuilder> builder : builders) {
            builder.apply(clusterBuilder.addTable()).end();
        }

        return clusterBuilder.build();
    }

    private static DataProvider<Object[]> identityTableProvider(String node) {
        List<Object[]> rows = IntStream.range(0, 5)
                .mapToObj(i -> new Object[]{node, String.valueOf(i)}).collect(Collectors.toList());
        return DataProvider.fromCollection(rows);
    }
}
