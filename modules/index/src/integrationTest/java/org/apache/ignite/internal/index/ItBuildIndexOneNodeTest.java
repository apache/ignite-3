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

package org.apache.ignite.internal.index;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.table.TableTestUtils.getIndexStrict;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.MakeIndexAvailableEventParameters;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.replication.request.BuildIndexReplicaRequest;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.table.Table;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Integration test for testing the building of an index in a single node cluster. */
@SuppressWarnings("resource")
public class ItBuildIndexOneNodeTest extends BaseSqlIntegrationTest {
    private static final String TABLE_NAME = "TEST_TABLE";

    private static final String ZONE_NAME = zoneName(TABLE_NAME);

    private static final String INDEX_NAME = "TEST_INDEX";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @AfterEach
    void tearDown() {
        sql("DROP TABLE IF EXISTS " + TABLE_NAME);
        sql("DROP ZONE IF EXISTS " + ZONE_NAME);

        CLUSTER.runningNodes().forEach(IgniteImpl::stopDroppingMessages);
    }

    @Test
    void testRecoverBuildingIndex() throws Exception {
        createZoneAndTable(ZONE_NAME, TABLE_NAME, 1, 1);

        insertPeople(TABLE_NAME, new Person(0, "0", 10.0));

        CompletableFuture<Void> awaitBuildIndexReplicaRequest = new CompletableFuture<>();

        node().dropMessages((s, networkMessage) -> {
            if (networkMessage instanceof BuildIndexReplicaRequest) {
                awaitBuildIndexReplicaRequest.complete(null);

                return true;
            }

            return false;
        });

        CompletableFuture<Void> createIndexFuture = runAsync(() -> createIndex(TABLE_NAME, INDEX_NAME, "ID"));

        assertThat(awaitBuildIndexReplicaRequest, willCompleteSuccessfully());

        assertFalse(isIndexAvailable(node(), INDEX_NAME));

        // Let's restart the node.
        CLUSTER.stopNode(0);
        CLUSTER.startNode(0);

        assertThat(createIndexFuture, willThrow(SqlException.class, containsString("Operation has been cancelled (node is stopping)")));
        awaitIndexesBecomeAvailable(node(), INDEX_NAME);
    }

    @Test
    void testBuildingIndex() throws Exception {
        createZoneAndTable(ZONE_NAME, TABLE_NAME, 1, 1);

        insertPeople(TABLE_NAME, new Person(0, "0", 10.0));

        createIndexForSalaryFieldAndWaitBecomeAvailable();

        // Now let's check the data itself.
        assertQuery(format("SELECT * FROM {} WHERE salary > 0.0", TABLE_NAME))
                .matches(containsIndexScan(SqlCommon.DEFAULT_SCHEMA_NAME, TABLE_NAME, INDEX_NAME))
                .returns(0, "0", 10.0)
                .check();
    }

    @Test
    void testBuildingIndexAndInsertIntoTableInParallel() throws Exception {
        AtomicInteger nextPersonId = new AtomicInteger();

        createTableAndInsertManyPeople(nextPersonId);

        CompletableFuture<Void> awaitIndexBecomeAvailableEventAsync = awaitIndexBecomeAvailableEventAsync(node(), INDEX_NAME);

        CompletableFuture<Integer> insertIntoTableFuture = runAsyncWithWaitStartExecution(() -> {
            int insertionsCount = 0;
            int batchSize = 10;

            while (!awaitIndexBecomeAvailableEventAsync.isDone()) {
                insertPeople(TABLE_NAME, createPeopleBatch(nextPersonId, batchSize));

                insertionsCount += batchSize;
            }

            return insertionsCount;
        });

        createIndexForSalaryFieldAndWaitBecomeAvailable();

        assertThat(awaitIndexBecomeAvailableEventAsync, willCompleteSuccessfully());
        assertThat(insertIntoTableFuture, willBe(greaterThan(0)));

        // Now let's check the data itself.
        assertQuery(format("SELECT * FROM {} WHERE salary > 0.0", TABLE_NAME))
                .matches(containsIndexScan(SqlCommon.DEFAULT_SCHEMA_NAME, TABLE_NAME, INDEX_NAME))
                .returnRowCount(nextPersonId.get())
                .check();
    }

    @Test
    void testBuildingIndexAndUpdateIntoTableInParallel() throws Exception {
        AtomicInteger nextPersonId = new AtomicInteger();

        createTableAndInsertManyPeople(nextPersonId);

        CompletableFuture<Void> awaitIndexBecomeAvailableEventAsync = awaitIndexBecomeAvailableEventAsync(node(), INDEX_NAME);

        CompletableFuture<Integer> updateIntoTableFuture = runAsyncWithWaitStartExecution(() -> {
            int updatedRowCount = 0;

            while (!awaitIndexBecomeAvailableEventAsync.isDone() && updatedRowCount < nextPersonId.get()) {
                int batchSize = Math.min(10, nextPersonId.get() - updatedRowCount);
                int finalUpdateRowCount = updatedRowCount;

                Person[] people = IntStream.range(0, batchSize)
                        .map(i -> finalUpdateRowCount + i)
                        .mapToObj(personId -> new Person(personId, updatePersonName(personId), 100.0 + personId))
                        .toArray(Person[]::new);

                updatePeople(TABLE_NAME, people);

                updatedRowCount += batchSize;
            }

            return updatedRowCount;
        });

        createIndexForSalaryFieldAndWaitBecomeAvailable();

        assertThat(awaitIndexBecomeAvailableEventAsync, willCompleteSuccessfully());
        assertThat(updateIntoTableFuture, willBe(greaterThan(0)));

        // Now let's check the data itself.
        QueryChecker queryChecker = assertQuery(format("SELECT NAME FROM {} WHERE salary > 0.0 ORDER BY ID ASC", TABLE_NAME))
                .matches(containsIndexScan(SqlCommon.DEFAULT_SCHEMA_NAME, TABLE_NAME, INDEX_NAME))
                .ordered();

        int updatedRowCount = updateIntoTableFuture.join();

        IntStream.range(0, nextPersonId.get())
                .mapToObj(personId -> personId < updatedRowCount ? updatePersonName(personId) : insertPersonName(personId))
                .forEach(queryChecker::returns);

        queryChecker.check();
    }

    @Test
    void testBuildingIndexAndDeleteFromTableInParallel() throws Exception {
        AtomicInteger nextPersonId = new AtomicInteger();

        createTableAndInsertManyPeople(nextPersonId);

        CompletableFuture<Void> awaitIndexBecomeAvailableEventAsync = awaitIndexBecomeAvailableEventAsync(node(), INDEX_NAME);

        CompletableFuture<Integer> deleteFromTableFuture = runAsyncWithWaitStartExecution(() -> {
            int deletedRowCount = 0;

            while (!awaitIndexBecomeAvailableEventAsync.isDone() && deletedRowCount < nextPersonId.get()) {
                int batchSize = Math.min(10, nextPersonId.get() - deletedRowCount);
                int finalDeletedRowCount = deletedRowCount;

                int[] personIds = IntStream.range(0, batchSize)
                        .map(i -> finalDeletedRowCount + i)
                        .toArray();

                deletePeople(TABLE_NAME, personIds);

                deletedRowCount += batchSize;
            }

            return deletedRowCount;
        });

        createIndexForSalaryFieldAndWaitBecomeAvailable();

        assertThat(awaitIndexBecomeAvailableEventAsync, willCompleteSuccessfully());
        assertThat(deleteFromTableFuture, willBe(greaterThan(0)));

        // Now let's check the data itself.
        assertQuery(format("SELECT NAME FROM {} WHERE salary > 0.0", TABLE_NAME))
                .matches(containsIndexScan(SqlCommon.DEFAULT_SCHEMA_NAME, TABLE_NAME, INDEX_NAME))
                .returnRowCount(nextPersonId.get() - deleteFromTableFuture.join())
                .check();
    }

    @Test
    void testBuildingIndexWithUpdateSchema() throws Exception {
        createZoneAndTable(ZONE_NAME, TABLE_NAME, 1, 1);

        insertPeople(TABLE_NAME, new Person(0, "0", 10.0));

        sql(format("ALTER TABLE {} ADD COLUMN SURNAME VARCHAR DEFAULT 'foo'", TABLE_NAME));

        String indexName0 = INDEX_NAME + 0;
        String indexName1 = INDEX_NAME + 1;

        createIndex(TABLE_NAME, indexName0, "SALARY");
        createIndex(TABLE_NAME, indexName1, "SURNAME");

        awaitIndexesBecomeAvailable(node(), indexName0);
        awaitIndexesBecomeAvailable(node(), indexName1);

        // Hack so that we can wait for the index to be added to the sql planner.
        waitForReadTimestampThatObservesMostRecentCatalog();

        assertQuery(format("SELECT * FROM {} WHERE salary > 0.0", TABLE_NAME))
                .matches(containsIndexScan(SqlCommon.DEFAULT_SCHEMA_NAME, TABLE_NAME, indexName0))
                .returns(0, "0", 10.0, "foo")
                .check();

        assertQuery(format("SELECT * FROM {} WHERE SURNAME = 'foo'", TABLE_NAME))
                .matches(containsIndexScan(SqlCommon.DEFAULT_SCHEMA_NAME, TABLE_NAME, indexName1))
                .returns(0, "0", 10.0, "foo")
                .check();
    }

    @Test
    void testBuildingIndexWithUpdateSchemaAfterCreateIndex() {
        createZoneAndTable(ZONE_NAME, TABLE_NAME, 1, 1);

        insertPeople(TABLE_NAME, new Person(0, "0", 10.0));

        String columName = "SURNAME";

        sql(format("ALTER TABLE {} ADD COLUMN {} VARCHAR DEFAULT 'foo'", TABLE_NAME, columName));

        // Hack to prevent the index from going into status BUILDING until we update the default value for the column.
        Transaction rwTx = node().transactions().begin(new TransactionOptions().readOnly(false));

        CompletableFuture<Void> createIndexFuture;
        try {
            createIndexFuture = runAsync(() -> createIndex(TABLE_NAME, INDEX_NAME, columName));

            sql(format("ALTER TABLE {} ALTER COLUMN {} SET DEFAULT 'bar'", TABLE_NAME, columName));
        } finally {
            rwTx.commit();
        }

        assertThat(createIndexFuture, willCompleteSuccessfully());

        assertQuery(format("SELECT * FROM {} WHERE SURNAME = 'foo'", TABLE_NAME))
                .matches(containsIndexScan(SqlCommon.DEFAULT_SCHEMA_NAME, TABLE_NAME, INDEX_NAME))
                .returns(0, "0", 10.0, "foo")
                .check();
    }

    @Test
    void testBuildIndexAfterDisasterRecovery() throws Exception {
        createZoneAndTable(ZONE_NAME, TABLE_NAME, 1, 1);

        insertPeople(TABLE_NAME, new Person(0, "0", 10.0));

        createIndexForSalaryFieldAndWaitBecomeAvailable();

        // Not a fair reproduction of disaster recovery, but simple.
        int partitionId = 0;
        setNextRowIdToBuild(INDEX_NAME, partitionId, RowId.lowestRowId(partitionId));

        CLUSTER.stopNode(0);
        CLUSTER.startNode(0);

        assertTrue(waitForCondition(() -> indexStorage(INDEX_NAME, partitionId).getNextRowIdToBuild() == null, SECONDS.toMillis(5)));

        // Now let's check the data itself.
        assertQuery(format("SELECT * FROM {} WHERE salary > 0.0", TABLE_NAME))
                .matches(containsIndexScan(SqlCommon.DEFAULT_SCHEMA_NAME, TABLE_NAME, INDEX_NAME))
                .returns(0, "0", 10.0)
                .check();
    }

    private static IgniteImpl node() {
        return CLUSTER.node(0);
    }

    private static CompletableFuture<Void> awaitIndexBecomeAvailableEventAsync(IgniteImpl ignite, String indexName) {
        var future = new CompletableFuture<Void>();

        CatalogManager catalogManager = ignite.catalogManager();

        catalogManager.listen(CatalogEvent.INDEX_AVAILABLE, parameters -> {
            try {
                int indexId = ((MakeIndexAvailableEventParameters) parameters).indexId();
                int catalogVersion = parameters.catalogVersion();

                CatalogIndexDescriptor index = catalogManager.index(indexId, catalogVersion);

                assertNotNull(index, "indexId=" + indexId + ", catalogVersion=" + catalogVersion);

                if (indexName.equals(index.name())) {
                    future.complete(null);

                    return trueCompletedFuture();
                }

                return falseCompletedFuture();
            } catch (Throwable t) {
                future.completeExceptionally(t);

                return failedFuture(t);
            }
        });

        return future;
    }

    private static void createTableAndInsertManyPeople(AtomicInteger nextPersonId) {
        createZoneOnlyIfNotExists(ZONE_NAME, 1, 1, DEFAULT_STORAGE_PROFILE);
        // Use hash index for primary key, otherwise if sorted index exists,
        // the optimizer might choose it (the primary key index) instead of an existing sorted one.
        sql(format(
                "CREATE TABLE IF NOT EXISTS {} (id INT, name VARCHAR, salary DOUBLE, PRIMARY KEY USING HASH (id)) WITH PRIMARY_ZONE='{}'",
                TABLE_NAME, ZONE_NAME
        ));

        insertPeople(TABLE_NAME, createPeopleBatch(nextPersonId, 100 * IndexBuilder.BATCH_SIZE));
    }

    private static void createIndexForSalaryFieldAndWaitBecomeAvailable() throws Exception {
        createIndexAndWaitBecomeAvailable(INDEX_NAME, "SALARY");
    }

    private static void createIndexAndWaitBecomeAvailable(String indexName, String columnName) throws Exception {
        createIndex(TABLE_NAME, indexName, columnName);

        // Hack so that we can wait for the index to be added to the sql planner.
        awaitIndexesBecomeAvailable(node(), indexName);
        waitForReadTimestampThatObservesMostRecentCatalog();
    }

    private static Person[] createPeopleBatch(AtomicInteger nextPersonId, int batchSize) {
        return IntStream.range(0, batchSize)
                .map(i -> nextPersonId.getAndIncrement())
                .mapToObj(personId -> new Person(personId, insertPersonName(personId), 10.0 + personId))
                .toArray(Person[]::new);
    }

    private static String insertPersonName(int personId) {
        return "person" + personId;
    }

    private static String updatePersonName(int personId) {
        return "personUpd" + personId;
    }

    private static <T> CompletableFuture<T> runAsyncWithWaitStartExecution(Callable<T> fun) {
        var startFunFuture = new CompletableFuture<Void>();

        CompletableFuture<T> future = runAsync(() -> {
            startFunFuture.complete(null);

            return fun.call();
        });

        assertThat(startFunFuture, willCompleteSuccessfully());

        return future;
    }

    private static void setNextRowIdToBuild(String indexName, int partitionId, @Nullable RowId nextRowIdToBuild) {
        CatalogIndexDescriptor indexDescriptor = indexDescriptor(indexName);

        MvPartitionStorage mvPartitionStorage = mvPartitionStorage(indexDescriptor, partitionId);
        IndexStorage indexStorage = indexStorage(indexDescriptor, partitionId);

        CompletableFuture<Void> flushFuture = runAsync(() -> {
            mvPartitionStorage.runConsistently(locker -> {
                indexStorage.setNextRowIdToBuild(nextRowIdToBuild);

                return null;
            });
        }, "test-flusher");

        assertThat(flushFuture, willCompleteSuccessfully());
    }

    private static TableViewInternal tableViewInternal(int tableId) {
        CompletableFuture<List<Table>> tablesFuture = node().tables().tablesAsync();

        assertThat(tablesFuture, willCompleteSuccessfully());

        TableViewInternal tableViewInternal = tablesFuture.join().stream()
                .map(TestWrappers::unwrapTableViewInternal)
                .filter(table -> table.tableId() == tableId)
                .findFirst()
                .orElse(null);

        assertNotNull(tableViewInternal, "tableId=" + tableId);

        return tableViewInternal;
    }

    private static CatalogIndexDescriptor indexDescriptor(String indexName) {
        IgniteImpl node = node();

        return getIndexStrict(node.catalogManager(), indexName, node.clock().nowLong());
    }

    private static IndexStorage indexStorage(String indexName, int partitionId) {
        return indexStorage(indexDescriptor(indexName), partitionId);
    }

    private static IndexStorage indexStorage(CatalogIndexDescriptor indexDescriptor, int partitionId) {
        TableViewInternal tableViewInternal = tableViewInternal(indexDescriptor.tableId());

        int indexId = indexDescriptor.id();

        IndexStorage indexStorage = tableViewInternal.internalTable().storage().getIndex(partitionId, indexId);

        assertNotNull(indexStorage, String.format("indexId=%s, partitionId=%s", indexId, partitionId));

        return indexStorage;
    }

    private static MvPartitionStorage mvPartitionStorage(CatalogIndexDescriptor indexDescriptor, int partitionId) {
        int tableId = indexDescriptor.tableId();

        TableViewInternal tableViewInternal = tableViewInternal(tableId);

        MvTableStorage mvTableStorage = tableViewInternal.internalTable().storage();

        MvPartitionStorage mvPartitionStorage = mvTableStorage.getMvPartition(partitionId);
        assertNotNull(mvPartitionStorage, String.format("tableId=%s, partitionId=%s", tableId, partitionId));

        return mvPartitionStorage;
    }
}
