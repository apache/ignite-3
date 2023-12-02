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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.MakeIndexAvailableEventParameters;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.table.distributed.replication.request.BuildIndexReplicaRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** Integration test for testing the building of an index in a single node cluster. */
public class ItBuildIndexOneNodeTest extends BaseSqlIntegrationTest {
    private static final String TABLE_NAME = "TEST_TABLE";

    private static final String ZONE_NAME = zoneName(TABLE_NAME);

    private static final String INDEX_NAME = "TEST_INDEX";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeEach
    void setup() {
        setAwaitIndexAvailability(false);
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

        createIndex(TABLE_NAME, INDEX_NAME, "ID");

        assertThat(awaitBuildIndexReplicaRequest, willCompleteSuccessfully());

        assertFalse(isIndexAvailable(node(), INDEX_NAME));

        // Let's restart the node.
        CLUSTER.stopNode(0);
        CLUSTER.startNode(0);

        awaitIndexesBecomeAvailable(node(), INDEX_NAME);
    }

    @Test
    void testBuildingIndex() throws Exception {
        createZoneAndTable(ZONE_NAME, TABLE_NAME, 1, 1);

        insertPeople(TABLE_NAME, new Person(0, "0", 10.0));

        createIndexForSalaryFieldAndWaitBecomeAvailable();

        // Now let's check the data itself.
        assertQuery(format("SELECT * FROM {} WHERE salary > 0.0", TABLE_NAME))
                .matches(containsIndexScan("PUBLIC", TABLE_NAME, INDEX_NAME))
                .returns(0, "0", 10.0)
                .check();
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-20117")
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
                .matches(containsIndexScan("PUBLIC", TABLE_NAME, INDEX_NAME))
                .returnRowCount(nextPersonId.get())
                .check();
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-20117")
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
                .matches(containsIndexScan("PUBLIC", TABLE_NAME, INDEX_NAME))
                .ordered();

        int updatedRowCount = updateIntoTableFuture.join();

        IntStream.range(0, nextPersonId.get())
                .mapToObj(personId -> personId < updatedRowCount ? updatePersonName(personId) : insertPersonName(personId))
                .forEach(queryChecker::returns);

        queryChecker.check();
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-20117")
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
                .matches(containsIndexScan("PUBLIC", TABLE_NAME, INDEX_NAME))
                .returnRowCount(nextPersonId.get() - deleteFromTableFuture.join())
                .check();
    }

    private static IgniteImpl node() {
        return CLUSTER.node(0);
    }

    private static CompletableFuture<Void> awaitIndexBecomeAvailableEventAsync(IgniteImpl ignite, String indexName) {
        var future = new CompletableFuture<Void>();

        CatalogManager catalogManager = ignite.catalogManager();

        catalogManager.listen(CatalogEvent.INDEX_AVAILABLE, (parameters, exception) -> {
            if (exception != null) {
                future.completeExceptionally(exception);

                return failedFuture(exception);
            }

            try {
                int indexId = ((MakeIndexAvailableEventParameters) parameters).indexId();
                int catalogVersion = parameters.catalogVersion();

                CatalogIndexDescriptor index = catalogManager.index(indexId, catalogVersion);

                assertNotNull(index, "indexId=" + indexId + ", catalogVersion=" + catalogVersion);

                if (indexName.equals(index.name())) {
                    future.complete(null);

                    return completedFuture(true);
                }

                return completedFuture(false);
            } catch (Throwable t) {
                future.completeExceptionally(t);

                return failedFuture(t);
            }
        });

        return future;
    }

    private static void createTableAndInsertManyPeople(AtomicInteger nextPersonId) {
        createZoneAndTable(ZONE_NAME, TABLE_NAME, 1, 1);

        insertPeople(TABLE_NAME, createPeopleBatch(nextPersonId, 100 * IndexBuilder.BATCH_SIZE));
    }

    private static void createIndexForSalaryFieldAndWaitBecomeAvailable() throws Exception {
        createIndex(TABLE_NAME, INDEX_NAME, "SALARY");

        // Hack so that we can wait for the index to be added to the sql planner.
        awaitIndexesBecomeAvailable(node(), INDEX_NAME);
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
}
