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

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.ignite.internal.ClusterPerClassIntegrationTest.isIndexAvailable;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.index.IndexBuildTestUtils.INDEX_NAME;
import static org.apache.ignite.internal.index.IndexBuildTestUtils.TABLE_NAME;
import static org.apache.ignite.internal.index.IndexBuildTestUtils.createIndex;
import static org.apache.ignite.internal.index.IndexBuildTestUtils.createTestTable;
import static org.apache.ignite.internal.index.IndexBuildTestUtils.rowsInIndex;
import static org.apache.ignite.internal.index.IndexBuildTestUtils.verifyNoNodesHaveAnythingInIndex;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.table.distributed.replicator.StaleTransactionOperationException;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ExecutorServiceExtension.class)
class ItIndexBuildCompletenessTest extends ClusterPerTestIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void staleOperationIssuedLaterThanIndexBuildIsFinishedDoesNotWriteToIndex() {
        createTestTable(cluster, 1, 1);

        Transaction tx = cluster.node(0).transactions().begin();

        createIndex(cluster, INDEX_NAME);

        simulateCoordinatorLeaveToMakeIndexBuildStart();

        await("Index must become available in time")
                .atMost(10, SECONDS)
                .until(() -> isIndexAvailable(unwrapIgniteImpl(cluster.aliveNode()), INDEX_NAME));

        KeyValueView<Integer, Integer> kvView = cluster.aliveNode().tables().table(TABLE_NAME).keyValueView(Integer.class, Integer.class);
        //noinspection ThrowableNotThrown
        assertThrowsWithCause(() -> {
            kvView.put(tx, 1, 11);
            tx.commit();
        }, StaleTransactionOperationException.class);

        verifyNoNodesHaveAnythingInIndex(cluster, initialNodes());
    }

    private void simulateCoordinatorLeaveToMakeIndexBuildStart() {
        TxManager txManager = unwrapIgniteImpl(cluster.aliveNode()).txManager();
        ((TxManagerImpl) txManager).clearLocalRwTxCounter();
    }

    @Test
    void raceBetweenIndexBuildAndWriteFromDeadCoordinatorDoesNotCauseIndexIncompleteness() {
        createTestTable(cluster, 1, 1);

        int operationCount = 100;

        List<Transaction> transactions = IntStream.range(0, operationCount)
                .mapToObj(n -> cluster.node(0).transactions().begin())
                .collect(toUnmodifiableList());

        createIndex(cluster, INDEX_NAME);

        simulateCoordinatorLeaveToMakeIndexBuildStart();

        KeyValueView<Integer, Integer> kvView = cluster.aliveNode().tables().table(TABLE_NAME).keyValueView(Integer.class, Integer.class);

        int failedOperations = 0;
        for (int i = 0; i < operationCount; i++) {
            Transaction tx = transactions.get(i);
            try {
                kvView.put(tx, i, 42);
                tx.commit();
            } catch (RuntimeException e) {
                if (ExceptionUtils.hasCause(e, StaleTransactionOperationException.class)) {
                    failedOperations++;
                } else {
                    throw e;
                }
            }
        }

        await("Index must become available in time")
                .atMost(10, SECONDS)
                .until(() -> isIndexAvailable(unwrapIgniteImpl(cluster.aliveNode()), INDEX_NAME));

        int successfulOperations = transactions.size() - failedOperations;

        assertAll(
                () -> assertThat(rowsInIndex(cluster.aliveNode()), is(successfulOperations)),
                () -> assertThat((long) rowsInIndex(cluster.aliveNode()), is(rowCountInTable()))
        );
    }

    private long rowCountInTable() {
        try (ResultSet<SqlRow> resultSet = cluster.aliveNode().sql().execute(null, "SELECT COUNT(*) FROM " + TABLE_NAME)) {
            return resultSet.next().longValue(0);
        }
    }

    @Test
    void raceBetweenIndexBuildAndWriteFromDeadCoordinatorDoesNotCauseIndexIncompletenessForImplicitTransactions(
            @InjectExecutorService ExecutorService executor
    ) {
        createTestTable(cluster, 1, 1);

        CompletableFuture<Void> startedOperationsFuture = new CompletableFuture<>();

        CompletableFuture<Void> indexBuildCompleted = startedOperationsFuture.thenRunAsync(() -> {
            createIndex(cluster, INDEX_NAME);

            simulateCoordinatorLeaveToMakeIndexBuildStart();

            await("Index must become available in time")
                    .atMost(10, SECONDS)
                    .until(() -> isIndexAvailable(unwrapIgniteImpl(cluster.aliveNode()), INDEX_NAME));
        }, executor);

        KeyValueView<Integer, Integer> kvView = cluster.aliveNode().tables().table(TABLE_NAME).keyValueView(Integer.class, Integer.class);

        int failedOperations = 0;
        int i = 0;
        while (!indexBuildCompleted.isDone()) {
            try {
                kvView.put(null, i, 42);
            } catch (RuntimeException e) {
                if (ExceptionUtils.hasCause(e, StaleTransactionOperationException.class)) {
                    failedOperations++;
                } else {
                    throw e;
                }
            }

            if (i == 0) {
                startedOperationsFuture.complete(null);
            }

            i++;
        }

        int successfulOperations = i - failedOperations;

        assertAll(
                () -> assertThat(rowsInIndex(cluster.aliveNode()), is(successfulOperations)),
                () -> assertThat((long) rowsInIndex(cluster.aliveNode()), is(rowCountInTable()))
        );
    }
}
