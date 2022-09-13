/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.runner.app.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.HashUtils;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests thin client connecting to a real server node.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItThinClientConnectionTest extends ItAbstractThinClientTest {
    /**
     * Check that thin client can connect to any server node and work with table API.
     */
    @Test
    void testThinClientConnectsToServerNodesAndExecutesBasicTableOperations() throws Exception {
        for (var addr : getClientAddresses()) {
            try (var client = IgniteClient.builder().addresses(addr).build()) {
                List<Table> tables = client.tables().tables();
                assertEquals(1, tables.size());

                Table table = tables.get(0);
                assertEquals(String.format("%s.%s", SCHEMA_NAME, TABLE_NAME), table.name());

                var tuple = Tuple.create().set(COLUMN_KEY, 1).set(COLUMN_VAL, "Hello");
                var keyTuple = Tuple.create().set(COLUMN_KEY, 1);

                RecordView<Tuple> recView = table.recordView();

                recView.upsert(null, tuple);
                assertEquals("Hello", recView.get(null, keyTuple).stringValue(COLUMN_VAL));

                var kvView = table.keyValueView();
                assertEquals("Hello", kvView.get(null, keyTuple).stringValue(COLUMN_VAL));

                var pojoView = table.recordView(TestPojo.class);
                assertEquals("Hello", pojoView.get(null, new TestPojo(1)).val);

                assertTrue(recView.delete(null, keyTuple));

                List<ClusterNode> nodes = client.connections();
                assertEquals(1, nodes.size());
                assertThat(nodes.get(0).name(), startsWith("itcct_n_"));
            }
        }
    }

    /**
     * TODO: Temporary test for IGNITE-17395.
     */
    @Test
    void testAssignments() throws NodeStoppingException {
        TableManager tblMgr = (TableManager) server().tables();
        TableImpl tbl = (TableImpl) tblMgr.tables().get(0);

        // We have node info on the client.
        for (var node : server().clusterNodes()) {
            System.out.println(node.name() + " - " + node.id() + " " + node.address());
        }

        System.out.println();

        // We will pass assignments (partition -> nodeId array).
        List<String> assignments = tblMgr.assignments(tbl.tableId());
        System.out.println(assignments.size());

        for (String assignment : assignments) {
            System.out.println(assignment);
        }

        // TBD: Calculate partition per key.
        int colocationHash = HashUtils.hash32(1, 0);
        int partitionManual = colocationHash % assignments.size();

        int partition = tbl.partition(Tuple.create().set("KEY", 1));
        String leaderNodeId = assignments.get(partition);

        System.out.println("partition = " + partition + " partitionManual = " + partitionManual + ", leader = " + leaderNodeId);
    }
}
