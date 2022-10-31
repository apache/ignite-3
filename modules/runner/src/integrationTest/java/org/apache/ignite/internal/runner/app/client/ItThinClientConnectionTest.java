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

package org.apache.ignite.internal.runner.app.client;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.schema.testutils.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.testutils.builder.SchemaBuilders;
import org.apache.ignite.internal.schema.testutils.definition.ColumnType;
import org.apache.ignite.internal.schema.testutils.definition.TableDefinition;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.lang.IgniteTuple3;
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
                assertEquals(TABLE_NAME, table.name());

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

    @Test
    public void testCustomTemporalPrecision() {
        TableDefinition tblDef = SchemaBuilders.tableBuilder("PUB", "TEMP").columns(
                SchemaBuilders.column("key", ColumnType.INT32).build(),
                SchemaBuilders.column("time", ColumnType.time(3)).asNullable(true).build(),
                SchemaBuilders.column("datetime", ColumnType.datetime(4)).asNullable(true).build(),
                SchemaBuilders.column("timestamp", ColumnType.timestamp(5)).asNullable(true).build()
        ).withPrimaryKey("key").build();

        var node = startedNodes.get(0);

        ((TableManager) node.tables()).createTableAsync(tblDef.name(), tblCh ->
                SchemaConfigurationConverter.convert(tblDef, tblCh).changeReplicas(1).changePartitions(10)
        ).join();

        var table = client().tables().table("TEMP");
        var view = table.recordView();

        var tuple = Tuple.create().set("key", 1)
                .set("time", LocalTime.of(1, 2))
                .set("datetime", LocalDateTime.of(2022, 1, 2, 3, 4))
                .set("timestamp", Instant.ofEpochSecond(123));

        view.upsert(null, tuple);
    }
}
