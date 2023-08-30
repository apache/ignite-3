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

package org.apache.ignite.internal.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.sql.engine.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test reads with specific timestamp.
 */
public class ItReadOnlyTransactionTest extends ClusterPerClassIntegrationTest {
    /** Table name. */
    private static final String TABLE_NAME = "tbl";

    private static final String ZONE_NAME = "ZONE_" + TABLE_NAME.toUpperCase();

    /** Gap in future to request a data. */
    private static final int FUTURE_GAP = 700;

    @BeforeEach
    public void beforeEach() {
        sql(IgniteStringFormatter.format("CREATE ZONE IF NOT EXISTS {} WITH REPLICAS={}, PARTITIONS={};",
                ZONE_NAME, nodes(), 10));
        sql(IgniteStringFormatter.format("CREATE TABLE {}(id INT PRIMARY KEY, val VARCHAR) WITH PRIMARY_ZONE='{}'",
                TABLE_NAME, ZONE_NAME));

        Ignite ignite = CLUSTER_NODES.get(0);

        ignite.transactions().runInTransaction(tx -> {
            for (int i = 0; i < 100; i++) {
                sql(tx, "INSERT INTO " + TABLE_NAME + " VALUES (?, ?)", i, "str " + i);
            }

            assertEquals(100, checkData(tx, id -> "str " + id));
        });

        assertEquals(100, checkData(null, id -> "str " + id));
    }

    @AfterEach
    public void afterEach() {
        sql(IgniteStringFormatter.format("DROP TABLE {}", TABLE_NAME));

        sql(IgniteStringFormatter.format("DROP ZONE {}", ZONE_NAME));
    }

    @Test
    public void testFutureRead() throws Exception {
        for (int i = 0; i < nodes(); i++) {
            Ignite ignite = CLUSTER_NODES.get(i);

            InternalTable internalTable = ((TableImpl) ignite.tables().table(TABLE_NAME)).internalTable();
            SchemaDescriptor schema = ((TableImpl) ignite.tables().table(TABLE_NAME)).schemaView().schema();
            HybridClock clock = ((IgniteImpl) ignite).clock();

            Collection<ClusterNode> nodes = ignite.clusterNodes();

            for (ClusterNode clusterNode : nodes) {
                CompletableFuture<BinaryRow> getFut = internalTable.get(createRowKey(schema, 100 + i), clock.now(), clusterNode);

                assertNull(getFut.join());
            }

            ArrayList<CompletableFuture<BinaryRow>> futs = new ArrayList<>(nodes.size());

            long startTime = System.currentTimeMillis();

            for (ClusterNode clusterNode : nodes) {
                CompletableFuture<BinaryRow> getFut = internalTable.get(
                        createRowKey(schema, 100 + i),
                        new HybridTimestamp(clock.now().getPhysical() + FUTURE_GAP, 0),
                        clusterNode
                );
                assertFalse(getFut.isDone());

                futs.add(getFut);
            }

            internalTable.insert(createRow(schema, 100 + i), null).get();

            log.info("Delay to create a new data record [node={}, delay={}]", ignite.name(), (System.currentTimeMillis() - startTime));

            assertTrue(System.currentTimeMillis() - startTime < FUTURE_GAP,
                    "Too long to execute [delay=" + (System.currentTimeMillis() - startTime) + ']');

            for (var getFut : futs) {
                assertNotNull(getFut.get(10, TimeUnit.SECONDS));
            }
        }

        assertTrue(IgniteTestUtils.waitForCondition(
                () -> checkData(null, id -> id < 100 ? ("str " + id) : ("new str " + id)) == 100 + nodes(),
                10_000
        ));
    }

    @Test
    public void testPastRead() throws Exception {
        for (int i = 0; i < nodes(); i++) {
            Ignite ignite = CLUSTER_NODES.get(i);

            InternalTable internalTable = ((TableImpl) ignite.tables().table(TABLE_NAME)).internalTable();
            SchemaDescriptor schema = ((TableImpl) ignite.tables().table(TABLE_NAME)).schemaView().schema();
            HybridClock clock = ((IgniteImpl) ignite).clock();

            Collection<ClusterNode> nodes = ignite.clusterNodes();

            for (ClusterNode clusterNode : nodes) {
                CompletableFuture<BinaryRow> getFut = internalTable.get(createRowKey(schema, i), clock.now(), clusterNode);

                assertNotNull(getFut.join());
            }

            var pastTs = clock.now();

            long startTime = System.currentTimeMillis();

            internalTable.delete(createRowKey(schema, i), null).get();

            for (ClusterNode clusterNode : nodes) {
                CompletableFuture<BinaryRow> getFut = internalTable.get(createRowKey(schema, i), clock.now(), clusterNode);

                assertNull(getFut.join());
            }

            log.info("Delay to remove a data record [node={}, delay={}]", ignite.name(), (System.currentTimeMillis() - startTime));

            for (ClusterNode clusterNode : nodes) {
                CompletableFuture<BinaryRow> getFut = internalTable.get(createRowKey(schema, i), pastTs, clusterNode);

                assertNotNull(getFut.join());
            }
        }

        assertEquals(100 - nodes(), checkData(null, id -> "str " + id));
    }

    private static Row createRow(SchemaDescriptor schema, int id) {
        RowAssembler rowBuilder = new RowAssembler(schema);

        rowBuilder.appendInt(id);
        rowBuilder.appendString("new str " + id);

        return Row.wrapBinaryRow(schema, rowBuilder.build());
    }

    private static Row createRowKey(SchemaDescriptor schema, int id) {
        RowAssembler rowBuilder = RowAssembler.keyAssembler(schema);

        rowBuilder.appendInt(id);

        return Row.wrapKeyOnlyBinaryRow(schema, rowBuilder.build());
    }

    /**
     * Check rows in the table {@link ItReadOnlyTransactionTest#TABLE_NAME}.
     *
     * @param tx Transaction. The parameter might be {@code null} for implicit transaction.
     * @param valueMapper Function to map a primary key to a column.
     * @return Count of rows in the table.
     */
    private static int checkData(Transaction tx, Function<Integer, String> valueMapper) {
        List<List<Object>> rows = sql(tx, "SELECT id, val FROM " + TABLE_NAME + " ORDER BY id");

        for (List<Object> row : rows) {
            var id = (Integer) row.get(0);

            assertEquals(valueMapper.apply(id), row.get(1));
        }

        return rows.size();
    }
}
