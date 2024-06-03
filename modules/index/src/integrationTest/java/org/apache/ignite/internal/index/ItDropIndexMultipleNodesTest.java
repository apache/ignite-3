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

import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.RemoveIndexEventParameters;
import org.apache.ignite.internal.catalog.events.StartBuildingIndexEventParameters;
import org.apache.ignite.internal.datareplication.network.replication.BuildIndexReplicaRequest;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.event.EventParameters;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test class for scenarios related to dropping of indices, executed on a multiple node cluster.
 */
@SuppressWarnings("resource")
public class ItDropIndexMultipleNodesTest extends BaseSqlIntegrationTest {
    private static final String TABLE_NAME = "TEST";

    private static final String INDEX_NAME = "TEST_IDX";

    @BeforeEach
    void createTable() {
        int partitions = initialNodes();

        int replicas = initialNodes();

        createTable(TABLE_NAME, replicas, partitions);
    }

    @AfterEach
    void cleanup() {
        CLUSTER.runningNodes().forEach(IgniteImpl::stopDroppingMessages);

        dropAllTables();
    }

    @Test
    void testActiveRwTransactionPreventsStoppingIndexFromBeingRemoved() {
        int indexId = createIndex();

        CompletableFuture<Void> indexRemovedFuture = indexRemovedFuture();

        IgniteImpl node = CLUSTER.aliveNode();

        // Start a transaction. We expect that the index will not be removed until this transaction completes.
        runInRwTransaction(node, tx -> {
            dropIndex();

            CatalogIndexDescriptor indexDescriptor = node.catalogManager().index(indexId, node.clock().nowLong());

            assertThat(indexDescriptor, is(notNullValue()));
            assertThat(indexDescriptor.status(), is(CatalogIndexStatus.STOPPING));
            assertThat(indexRemovedFuture, willTimeoutFast());
        });

        assertThat(indexRemovedFuture, willCompleteSuccessfully());
    }

    @Test
    void testWritingIntoStoppingIndex() {
        int indexId = createIndex();

        IgniteImpl node = CLUSTER.aliveNode();

        // Latch for waiting for the RW transaction to start before dropping the index.
        var startTransactionLatch = new CountDownLatch(1);
        // Latch for waiting for the index to be dropped, before inserting data in the transaction.
        var dropIndexLatch = new CountDownLatch(1);

        CompletableFuture<Void> dropIndexFuture = runAsync(() -> {
            try {
                startTransactionLatch.await(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new CompletionException(e);
            }

            dropIndex();

            CatalogIndexDescriptor indexDescriptor = node.catalogManager().index(indexId, node.clock().nowLong());

            assertThat(indexDescriptor, is(notNullValue()));
            assertThat(indexDescriptor.status(), is(CatalogIndexStatus.STOPPING));

            dropIndexLatch.countDown();
        });

        CompletableFuture<Void> insertDataIntoIndexTransaction = runAsync(() -> {
            runInRwTransaction(node, tx -> {
                startTransactionLatch.countDown();

                try {
                    dropIndexLatch.await(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new CompletionException(e);
                }

                // Insert data into a STOPPING index. We expect it to be inserted.
                insertPeople(tx, TABLE_NAME, new Person(239, "foo", 0));

                assertQuery((InternalTransaction) tx, String.format("SELECT id FROM %s WHERE NAME > 'a'", TABLE_NAME))
                        .matches(containsIndexScan("PUBLIC", TABLE_NAME, INDEX_NAME))
                        .returns(239)
                        .check();
            });
        });

        assertThat(dropIndexFuture, willCompleteSuccessfully());
        assertThat(insertDataIntoIndexTransaction, willCompleteSuccessfully());
    }

    /**
     * Tests the following scenario.
     *
     * <ol>
     *     <li>Transaction A is started that is expected to observe an index in the {@link CatalogIndexStatus#AVAILABLE} state;</li>
     *     <li>The index is dropped;</li>
     *     <li>Transaction B is started that is expected to observe the index in the {@link CatalogIndexStatus#STOPPING} state;</li>
     *     <li>Transaction B inserts data into the table and, therefore, into the index;</li>
     *     <li>Transaction A performs a scan that utilizes the index over the table and is expected to see the data written by
     *     Transaction B.</li>
     * </ol>
     */
    @Test
    void testWritingIntoStoppingIndexInDifferentTransactions() {
        int indexId = createIndex();

        IgniteImpl node = CLUSTER.aliveNode();

        // Latch that will be released when a transaction expected to observe the index in the AVAILABLE state is started.
        var startAvailableTransactionLatch = new CountDownLatch(1);
        // Latch that will be released when a transaction expected to observe the index in the STOPPING state is started.
        var insertDataTransactionLatch = new CountDownLatch(1);
        // Latch for waiting for the index to be dropped.
        var dropIndexLatch = new CountDownLatch(1);

        CompletableFuture<Void> dropIndexFuture = runAsync(() -> {
            // Wait for a transaction to start before dropping the index. This way, the dropped index will be stuck in the
            // STOPPING state until that transaction finishes.
            try {
                startAvailableTransactionLatch.await(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new CompletionException(e);
            }

            dropIndex();

            CatalogIndexDescriptor indexDescriptor = node.catalogManager().index(indexId, node.clock().nowLong());

            assertThat(indexDescriptor, is(notNullValue()));
            assertThat(indexDescriptor.status(), is(CatalogIndexStatus.STOPPING));

            dropIndexLatch.countDown();
        });

        CompletableFuture<Void> insertDataIntoIndexTransaction = runAsync(() -> {
            // Wait for the index to be dropped, so that the transaction will see the index in the STOPPING state.
            try {
                dropIndexLatch.await(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new CompletionException(e);
            }

            // Insert data into a STOPPING index. We expect it to be inserted.
            runInRwTransaction(node, tx -> insertPeople(tx, TABLE_NAME, new Person(239, "foo", 0)));

            insertDataTransactionLatch.countDown();
        });

        CompletableFuture<Void> readDataFromIndexTransaction = runAsync(() -> runInRwTransaction(node, tx -> {
            startAvailableTransactionLatch.countDown();

            // Wait for the transaction that will insert data into the stopping index.
            try {
                insertDataTransactionLatch.await(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new CompletionException(e);
            }

            assertQuery((InternalTransaction) tx, String.format("SELECT id FROM %s WHERE NAME > 'a'", TABLE_NAME))
                    .matches(containsIndexScan("PUBLIC", TABLE_NAME, INDEX_NAME))
                    .returns(239)
                    .check();
        }));

        assertThat(dropIndexFuture, willCompleteSuccessfully());
        assertThat(insertDataIntoIndexTransaction, willCompleteSuccessfully());
        assertThat(readDataFromIndexTransaction, willCompleteSuccessfully());
    }

    private static int createIndex() {
        createIndexBlindly();

        IgniteImpl node = CLUSTER.aliveNode();

        return node.catalogManager().aliveIndex(INDEX_NAME, node.clock().nowLong()).id();
    }

    private static void createIndexBlindly() {
        createIndex(TABLE_NAME, INDEX_NAME, "name");
    }

    @Test
    void testDropIndexAfterRegistering() {
        CatalogManager catalogManager = CLUSTER.aliveNode().catalogManager();

        populateTable();

        CompletableFuture<Void> indexRemovedFuture = indexRemovedFuture();

        runInRwTransaction(CLUSTER.aliveNode(), tx -> {
            // Create an index inside a transaction, this will prevent the index from building.
            try {
                CompletableFuture<Void> creationFuture = runAsync(ItDropIndexMultipleNodesTest::createIndexBlindly);

                assertTrue(waitForCondition(
                        () -> catalogManager.schema(catalogManager.latestCatalogVersion()).aliveIndex(INDEX_NAME) != null,
                        10_000
                ));

                dropIndex();

                assertThat(creationFuture, willCompleteSuccessfully());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new RuntimeException(e);
            }
        });

        assertThat(indexRemovedFuture, willCompleteSuccessfully());
    }

    @Test
    void testDropIndexDuringBuilding() {
        populateTable();

        // Block index building messages, this way index will never become AVAILABLE.
        CLUSTER.runningNodes().forEach(ignite -> ignite.dropMessages((id, message) -> message instanceof BuildIndexReplicaRequest));

        CompletableFuture<Void> indexBuildingFuture = indexBuildingFuture();

        CompletableFuture<Void> indexRemovedFuture = indexRemovedFuture();

        CompletableFuture<Void> creationFuture = runAsync(ItDropIndexMultipleNodesTest::createIndexBlindly);

        assertThat(indexBuildingFuture, willCompleteSuccessfully());

        dropIndex();

        assertThat(indexRemovedFuture, willCompleteSuccessfully());
        assertThat(creationFuture, willCompleteSuccessfully());
    }

    private static void dropIndex() {
        dropIndex(INDEX_NAME);
    }

    private static void runInRwTransaction(IgniteImpl node, Consumer<Transaction> action) {
        node.transactions().runInTransaction(action, new TransactionOptions().readOnly(false));
    }

    private static void populateTable() {
        int idx = 0;

        insertData(TABLE_NAME, List.of("ID", "NAME", "SALARY"), new Object[][]{
                {idx++, "Igor", 10.0d},
                {idx++, null, 15.0d},
                {idx++, "Ilya", 15.0d},
                {idx++, "Roma", 10.0d},
                {idx, "Roma", 10.0d}
        });
    }

    private static CompletableFuture<Void> indexBuildingFuture() {
        return indexEventFuture(CatalogEvent.INDEX_BUILDING, (StartBuildingIndexEventParameters parameters, CatalogService catalog) -> {
            CatalogIndexDescriptor indexDescriptor = catalog.index(parameters.indexId(), parameters.catalogVersion());

            return indexDescriptor != null && indexDescriptor.name().equals(INDEX_NAME);
        });
    }

    private static CompletableFuture<Void> indexRemovedFuture() {
        return indexEventFuture(CatalogEvent.INDEX_REMOVED, (RemoveIndexEventParameters parameters, CatalogService catalog) ->
                catalog.catalog(parameters.catalogVersion() - 1)
                        .indexes()
                        .stream()
                        .anyMatch(index -> index.name().equals(INDEX_NAME))
        );
    }

    private static <P extends EventParameters> CompletableFuture<Void> indexEventFuture(
            CatalogEvent event, BiPredicate<P, CatalogService> action
    ) {
        CatalogService catalog = CLUSTER.aliveNode().catalogManager();

        var result = new CompletableFuture<Void>();

        catalog.listen(event, EventListener.fromConsumer(parameters -> {
            if (action.test((P) parameters, catalog)) {
                result.complete(null);
            }
        }));

        return result;
    }
}
