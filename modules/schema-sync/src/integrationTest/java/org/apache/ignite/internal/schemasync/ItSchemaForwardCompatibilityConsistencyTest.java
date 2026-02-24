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

package org.apache.ignite.internal.schemasync;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.executeUpdate;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.bypassingThreadAssertions;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.IncompatibleSchemaException;
import org.apache.ignite.tx.Transaction;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@ExtendWith(ExecutorServiceExtension.class)
abstract class ItSchemaForwardCompatibilityConsistencyTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_NAME = "TEST_ZONE";

    private Ignite node0;

    @InjectExecutorService
    private ExecutorService executor;

    @Override
    protected abstract int initialNodes();

    @BeforeEach
    void prepare() {
        node0 = cluster.node(0);

        node0.sql()
                .execute(
                        "CREATE ZONE " + ZONE_NAME + " (REPLICAS " + initialNodes() + ") STORAGE PROFILES ['"
                                + DEFAULT_AIPERSIST_PROFILE_NAME + "']"
                )
                .close();
    }

    @ParameterizedTest
    @EnumSource(SinglePartitionOperation.class)
    void forwardIncompatibleSchemaChangesCannotBeCreatedBy1PcTransactions(SinglePartitionOperation operation) throws Exception {
        for (int attempt = 0; attempt < 20; attempt++) {
            testForwardIncompatibleSchemaChangesCannotBeCreatedBy1PcTransactions(operation, attempt);
        }
    }

    private void testForwardIncompatibleSchemaChangesCannotBeCreatedBy1PcTransactions(SinglePartitionOperation operation, int attempt)
            throws InterruptedException {
        String tableName = "TABLE" + attempt;

        createTable(tableName);

        CountDownLatch startedPutting = new CountDownLatch(1);
        AtomicBoolean performWrites = new AtomicBoolean(true);
        AtomicInteger rowsAdded = new AtomicInteger();

        CompletableFuture<Void> putterFuture = CompletableFuture.runAsync(() -> {
            try {
                makeImplicitPut(0, tableName, operation);
                rowsAdded.incrementAndGet();
            } finally {
                startedPutting.countDown();
            }

            for (int i = 1; performWrites.get(); i++) {
                makeImplicitPut(i, tableName, operation);
                rowsAdded.incrementAndGet();
            }
        }, executor);

        assertTrue(startedPutting.await(10, SECONDS));
        applyForwardIncompatibleSchemaChange(tableName);

        performWrites.set(false);
        assertThat(putterFuture, willCompleteSuccessfully());

        for (Ignite node : cluster.nodes()) {
            List<ReadResult> readResults = readRowsFromTable(node, tableName, rowsAdded.get());

            assertThatSchemaVersionIsConsistentWithCommitTs(readResults, lastSchemaChangeActivationTs(tableName));
        }
    }

    private void createTable(String tableName) {
        cluster.doInSession(0, session -> {
            executeUpdate(
                    "CREATE TABLE " + tableName + " (id INT PRIMARY KEY, val INT, column_to_drop INT) ZONE " + ZONE_NAME,
                    session
            );
        });
    }

    private void applyForwardIncompatibleSchemaChange(String tableName) {
        cluster.doInSession(0, session -> {
            executeUpdate("ALTER TABLE " + tableName + " DROP COLUMN column_to_drop", session);
        });
    }

    private HybridTimestamp lastSchemaChangeActivationTs(String tableName) {
        IgniteImpl igniteImpl = unwrapIgniteImpl(node0);

        Catalog currentCatalog = igniteImpl.catalogManager().activeCatalog(igniteImpl.clock().nowLong());

        CatalogTableDescriptor tableDescriptor = currentCatalog.table(QualifiedName.DEFAULT_SCHEMA_NAME, tableName);
        assertThat(tableDescriptor, is(notNullValue()));
        assertThat(tableDescriptor.latestSchemaVersion(), is(2));

        return HybridTimestamp.hybridTimestamp(currentCatalog.time());
    }

    private void makeImplicitPut(int value, String tableName, SinglePartitionOperation singlePartitionOperation) {
        makePutInTransaction(value, null, tableName, singlePartitionOperation);
    }

    private void makeExplicitPut(int value, String tableName, SinglePartitionOperation singlePartitionOperation) {
        node0.transactions().runInTransaction((Transaction tx) -> makePutInTransaction(value, tx, tableName, singlePartitionOperation));
    }

    private void makePutInTransaction(int value, @Nullable Transaction tx, String tableName, SinglePartitionOperation operation) {
        operation.put(tx, value, value, tableName, node0);
    }

    private static List<ReadResult> readRowsFromTable(Ignite node, String tableName, int expectedRowCount) {
        List<ReadResult> readResults = readRowsFromTable(node, tableName);

        assertThat(readResults, hasSize(equalTo(expectedRowCount)));

        return readResults;
    }

    private static List<ReadResult> readRowsFromTable(Ignite node, String tableName) {
        try (ResultSet<SqlRow> resultSet = node.sql().execute("SELECT * FROM " + tableName)) {
            while (resultSet.hasNext()) {
                resultSet.next();
            }
        }

        List<ReadResult> readResults = new ArrayList<>();

        IgniteImpl ignite = unwrapIgniteImpl(node);
        TableImpl table = unwrapTableImpl(requireNonNull(ignite.distributedTableManager().cachedTable(tableName)));

        for (int partitionIndex = 0; partitionIndex < DEFAULT_PARTITION_COUNT; partitionIndex++) {
            collectRowsFromPartition(table, partitionIndex, readResults);
        }

        return readResults;
    }

    private static void collectRowsFromPartition(TableImpl table, int partitionIndex, List<ReadResult> readResults) {
        MvPartitionStorage partitionStorage = table.internalTable().storage().getMvPartition(partitionIndex);

        if (partitionStorage != null) {
            bypassingThreadAssertions(() -> {
                try (PartitionTimestampCursor cursor = partitionStorage.scan(HybridTimestamp.MAX_VALUE)) {
                    for (ReadResult readResult : cursor) {
                        readResults.add(readResult);
                    }
                }
            });
        }
    }

    private static void assertThatSchemaVersionIsConsistentWithCommitTs(
            List<ReadResult> readResults,
            HybridTimestamp incompatibleChangeActivationTs
    ) {
        for (ReadResult readResult : readResults) {
            HybridTimestamp commitTimestamp = readResult.commitTimestamp();
            assertThat(commitTimestamp, is(notNullValue()));

            BinaryRow binaryRow = readResult.binaryRow();
            assertThat(binaryRow, is(notNullValue()));

            if (commitTimestamp.compareTo(incompatibleChangeActivationTs) < 0) {
                assertThat(binaryRow.schemaVersion(), is(1));
            } else {
                assertThat(
                        "According to commitTs, schema version must be 2, but it's still 1; this should never happen for "
                                + "forward incompatible schema changes",
                        binaryRow.schemaVersion(), is(2)
                );
            }
        }
    }

    @ParameterizedTest
    @EnumSource(SinglePartitionOperation.class)
    void forwardIncompatibleSchemaChangesCannotBeCreatedBy2PcTransactions(SinglePartitionOperation operation) throws Exception {
        for (int attempt = 0; attempt < 10; attempt++) {
            testForwardIncompatibleSchemaChangesCannotBeCreatedBy2PcTransactions(operation, attempt);
        }
    }

    private void testForwardIncompatibleSchemaChangesCannotBeCreatedBy2PcTransactions(SinglePartitionOperation operation, int attempt)
            throws InterruptedException {
        String tableName = "TABLE" + attempt;

        createTable(tableName);

        CountDownLatch startedPutting = new CountDownLatch(1);
        AtomicBoolean performWrites = new AtomicBoolean(true);
        AtomicInteger rowsAdded = new AtomicInteger();

        CompletableFuture<Void> putterFuture = CompletableFuture.runAsync(() -> {
            try {
                makeExplicitPut(0, tableName, operation);
                rowsAdded.incrementAndGet();
            } finally {
                startedPutting.countDown();
            }

            for (int i = 1; performWrites.get(); i++) {
                try {
                    makeExplicitPut(i, tableName, operation);
                    rowsAdded.incrementAndGet();
                } catch (Exception e) {
                    if (!hasCause(e, IncompatibleSchemaException.class)) {
                        throw e;
                    }
                }
            }
        }, executor);

        assertTrue(startedPutting.await(10, SECONDS));
        applyForwardIncompatibleSchemaChange(tableName);

        performWrites.set(false);
        assertThat(putterFuture, willCompleteSuccessfully());

        for (Ignite node : cluster.nodes()) {
            List<ReadResult> readResults = readRowsFromTable(node, tableName, rowsAdded.get());

            assertThatSchemaVersionIsConsistentWithCommitTs(readResults, lastSchemaChangeActivationTs(tableName));
        }
    }

    private static KeyValueView<Tuple, Tuple> kvView(String tableName, Ignite ignite) {
        return ignite.tables().table(tableName).keyValueView();
    }

    private static Tuple keyTuple(int key) {
        return Tuple.create().set("id", key);
    }

    private static Tuple valueTuple(int value) {
        return Tuple.create().set("val", value);
    }

    private enum SinglePartitionOperation {
        KV_SINGLE {
            @Override
            void put(@Nullable Transaction tx, int key, int value, String tableName, Ignite ignite) {
                kvView(tableName, ignite).put(tx, keyTuple(key), valueTuple(value));
            }
        },
        KV_BATCH {
            @Override
            void put(@Nullable Transaction tx, int key, int value, String tableName, Ignite ignite) {
                kvView(tableName, ignite).putAll(tx, Map.of(keyTuple(key), valueTuple(value)));
            }
        },
        SQL_SINGLE {
            @Override
            void put(@Nullable Transaction tx, int key, int value, String tableName, Ignite ignite) {
                ignite.sql()
                        .execute(tx, "INSERT INTO " + tableName + " (ID, VAL) VALUES (" + key + ", " + value + ")")
                        .close();
            }
        };

        abstract void put(@Nullable Transaction tx, int key, int value, String tableName, Ignite ignite);
    }
}
