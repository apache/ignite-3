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

package org.apache.ignite.internal.sql.engine.framework;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.sql.engine.prepare.Fragment;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for test execution runtime used in benchmarking.
 */
public class TestClusterTest extends BaseIgniteAbstractTest {

    private final DataProvider<Object[]> dataProvider = DataProvider.fromRow(
            new Object[]{42, UUID.randomUUID().toString()}, 3_333
    );

    // @formatter:off
    private final TestCluster cluster = TestBuilders.cluster().nodes("N1")
            .addTable()
            .name("T1")
            .distribution(IgniteDistributions.hash(List.of(0)))
            .addColumn("ID", NativeTypes.INT32)
            .addColumn("VAL", NativeTypes.stringOf(64))
            .defaultDataProvider(dataProvider)
            .addHashIndex()
            .name("IDX_ID")
            .addColumn("ID")
            .defaultDataProvider(dataProvider)
            .end()
            .end()
            .build();
    // @formatter:on

    @AfterEach
    public void stopCluster() throws Exception {
        cluster.stop();
    }

    /**
     * Runs a simple SELECT query.
     */
    @Test
    public void testSimpleQuery() {
        cluster.start();

        var gatewayNode = cluster.node("N1");
        var plan = gatewayNode.prepare("SELECT * FROM t1");

        for (var row : await(gatewayNode.executePlan(plan).requestNextAsync(10_000)).items()) {
            assertNotNull(row);
        }

        // Ensure the plan contains full table scan.
        assertTrue(plan instanceof MultiStepPlan);
        Fragment fragment = ((MultiStepPlan) plan).fragments().get(1);
        assertTrue(fragment.root().getInput(0) instanceof IgniteTableScan);
    }

    /**
     * Runs a SELECT query with condition.
     */
    @Test
    public void testQueryWithCondition() {
        cluster.start();

        TestNode gatewayNode = cluster.node("N1");
        QueryPlan plan = gatewayNode.prepare("SELECT * FROM t1 WHERE ID = 1");

        for (List<?> row : await(gatewayNode.executePlan(plan).requestNextAsync(10_000)).items()) {
            assertNotNull(row);
        }

        // Ensure the plan uses index.
        assertTrue(plan instanceof MultiStepPlan);
        Fragment fragment = ((MultiStepPlan) plan).fragments().get(1);
        assertTrue(fragment.root().getInput(0) instanceof IgniteIndexScan);
    }
}
