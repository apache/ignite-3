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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;

import java.util.List;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.framework.ImplicitTxContext;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class QueryRetryTest extends BaseIgniteAbstractTest {
    private TestCluster cluster;

    @BeforeEach
    public void init() {
        cluster = TestBuilders.cluster()
                .nodes("N1")
                .dataProvider("N1", "TEST_TBL", TestBuilders.tableScan(DataProvider.fromCollection(List.of())))
                .build();

        cluster.start();
    }

    @AfterEach
    public void tearDown() throws Exception {
        cluster.stop();
    }

    @Test
    void testQuery() {
        TestNode node = cluster.node("N1");

        node.initSchema("CREATE TABLE test_tbl (ID INT PRIMARY KEY, VAL INT, VAL2 INT)");

        QueryPlan plan = node.prepare("SELECT * FROM test_tbl");

        node.initSchema("ALTER TABLE test_tbl DROP COLUMN VAL2");
        ImplicitTxContext.INSTANCE.updateObservableTime(new HybridClockImpl().now());

        assertThrows(ConcurrentSchemaModificationException.class, () -> node.executePlan(plan), null);
    }

    @Test
    void testLookupQuery() throws Exception {
        TestNode node = cluster.node("N1");

        node.initSchema("CREATE TABLE test_tbl (ID INT PRIMARY KEY, VAL INT, VAL2 INT)");

        QueryPlan plan = node.prepare("SELECT * FROM test_tbl WHERE id = 1");

        node.initSchema("ALTER TABLE test_tbl DROP COLUMN VAL2");
        ImplicitTxContext.INSTANCE.updateObservableTime(new HybridClockImpl().now());

        assertThrows(ConcurrentSchemaModificationException.class, () -> node.executePlan(plan).requestNextAsync(10).get(), null);
    }
}