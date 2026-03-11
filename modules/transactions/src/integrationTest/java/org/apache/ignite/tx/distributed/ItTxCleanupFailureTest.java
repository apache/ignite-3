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

package org.apache.ignite.tx.distributed;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.tx.metrics.TransactionMetricsSource.METRIC_PENDING_WRITE_INTENTS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicaRequest;
import org.apache.ignite.internal.tx.metrics.TransactionMetricsSource;
import org.apache.ignite.internal.util.retry.KeyBasedRetryContext;
import org.apache.ignite.internal.util.retry.TimeoutState;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests verifying the retry behavior of transaction write intent cleanup
 * ({@link WriteIntentSwitchReplicaRequest}) in a 3-node Apache Ignite cluster.
 *
 * <p>Each test drops or intercepts {@link WriteIntentSwitchReplicaRequest} messages on all nodes
 * to simulate transient network failures during the cleanup phase that follows a committed
 * transaction. Tests verify that:
 * <ul>
 *     <li>cleanup is retried after a transient failure and eventually succeeds;</li>
 *     <li>retry timeouts grow monotonically between consecutive attempts;</li>
 *     <li>the retry context is cleaned up after successful cleanup;</li>
 *     <li>no unnecessary retries occur when the first cleanup attempt succeeds.</li>
 * </ul>
 *
 * <p>Each test creates a single-partition, 3-replica zone and table in {@link #setup()}.
 * Tests that require additional zones or tables create them locally.
 */
public class ItTxCleanupFailureTest extends ClusterPerTestIntegrationTest {
    /**
     * Thread name fragment identifying the thread that sends retry attempts.
     * Used to distinguish retried messages from the original cleanup attempt in message interceptors.
     */
    private static final String CLEANUP_THREAD_NAME = "tx-async-write-intent";

    /** Name of the default test table created in {@link #setup()}. */
    private static final String TABLE_NAME = "test_table";

    /** Number of replicas for all test zones. */
    private static final int REPLICAS = 3;

    /**
     * Creates a single-partition, 3-replica zone and a test table before each test.
     * Tests that require additional tables or zones create them locally.
     */
    @BeforeEach
    public void setup() {
        String zoneSql = "create zone test_zone (partitions 1, replicas " + REPLICAS
                + ") storage profiles ['" + DEFAULT_STORAGE_PROFILE + "']";
        String tableSql = "create table " + TABLE_NAME + " (key bigint primary key, val varchar(20)) zone TEST_ZONE";

        sql(zoneSql);
        sql(tableSql);
    }

    /**
     * Verifies that no retry occurs when the write intent cleanup succeeds on the first attempt.
     *
     * <p>Installs a message interceptor that counts {@link WriteIntentSwitchReplicaRequest}
     * messages without dropping any of them. After all write intents are resolved, asserts
     * that exactly one cleanup message was sent across all nodes.
     */
    @Test
    public void testNoRetryOnSuccessfulCleanup() {
        IgniteImpl node = anyNode();
        Transaction tx = node.transactions().begin();
        node.sql().execute(tx, "insert into " + TABLE_NAME + " (key, val) values (1, 'val-1')");

        AtomicInteger cleanupAttempts = new AtomicInteger();

        for (IgniteImpl n : runningNodesIter()) {
            n.dropMessages((dest, msg) -> {
                if (msg instanceof WriteIntentSwitchReplicaRequest) {
                    cleanupAttempts.incrementAndGet();
                }
                return false;
            });
        }

        tx.commitAsync();

        await().timeout(5, TimeUnit.SECONDS)
                .until(() -> pendingWriteIntents(node) == 0);

        await().timeout(1, SECONDS).until(() -> cleanupAttempts.get() >= 1);

        assertEquals(1, cleanupAttempts.get());
    }

    /**
     * Verifies that the write intent cleanup is retried after a single failure.
     *
     * <p>Drops the first {@link WriteIntentSwitchReplicaRequest} on all nodes to simulate
     * a transient failure. On the subsequent retry — arriving on the write intent switch
     * executor thread — captures a snapshot of the retry context across all nodes and records
     * how many nodes have an active entry for this transaction. After cleanup completes,
     * asserts that exactly one node had an active retry entry during the retry, and that
     * the retry context is fully cleaned up.
     */
    @Test
    public void testRetry() {
        IgniteImpl node = anyNode();
        Transaction tx = node.transactions().begin();
        node.sql().execute(tx, "insert into " + TABLE_NAME + " (key, val) values (1, 'val-1')");

        AtomicInteger failedCleanupAttempts = new AtomicInteger();

        List<KeyBasedRetryContext> retryContexts = new ArrayList<>();

        AtomicLong expectedSizeOfRetryContext = new AtomicLong(0);

        for (IgniteImpl n : runningNodesIter()) {
            retryContexts.add(((TxManagerImpl) n.txManager()).retryContext());

            n.dropMessages((dest, msg) -> {
                if (msg instanceof WriteIntentSwitchReplicaRequest) {
                    if (failedCleanupAttempts.get() == 0) {
                        // Makes cleanup fail on write intent switch attempt with replication timeout, on the first attempt.
                        return failedCleanupAttempts.incrementAndGet() == 1;
                    }

                    if (Thread.currentThread().getName().contains(CLEANUP_THREAD_NAME)) {
                        expectedSizeOfRetryContext.compareAndSet(0, retryContexts.stream()
                                .map(KeyBasedRetryContext::snapshot)
                                .filter(retryContextSnapshot -> retryContextSnapshot.size() == 1)
                                .count());
                    }
                }

                return false;
            });
        }

        tx.commitAsync();

        await().timeout(5, SECONDS).until(() -> failedCleanupAttempts.get() == 1);

        await().timeout(5, SECONDS).until(() -> pendingWriteIntents(node) == 0);

        assertEquals(1, expectedSizeOfRetryContext.get());

        await().timeout(5, SECONDS).until(() -> retryContexts.stream()
                .map(KeyBasedRetryContext::snapshot).allMatch(Map::isEmpty));
    }

    /**
     * Verifies that retry timeouts grow monotonically between consecutive retry attempts.
     *
     * <p>Drops the first 3 {@link WriteIntentSwitchReplicaRequest} messages arriving on the
     * write intent switch executor thread (identified by the executor's thread name prefix).
     * For each dropped attempt, captures the current timeout from the retry context snapshot.
     * After all drops and the final successful cleanup, asserts that the captured timeout
     * sequence is strictly increasing, confirming exponential backoff behavior.
     *
     * <p>Sampling is restricted to the write intent switch executor thread to ensure timeouts
     * are captured after the retry context has been advanced for the current attempt, rather
     * than observing a stale pre-drop state.
     */
    @Test
    public void testRetryWithGrowingTimeout() {
        IgniteImpl node = anyNode();
        Transaction tx = node.transactions().begin();
        node.sql().execute(tx, "insert into " + TABLE_NAME + " (key, val) values (1, 'val-1')");

        AtomicInteger failedCleanupAttempts = new AtomicInteger();

        List<KeyBasedRetryContext> retryContexts = new ArrayList<>();

        List<Integer> timeoutSamples = new CopyOnWriteArrayList<>();

        for (IgniteImpl n : runningNodesIter()) {
            TxManagerImpl txManager = (TxManagerImpl) n.txManager();
            retryContexts.add(txManager.retryContext());

            n.dropMessages((dest, msg) -> {
                if (msg instanceof WriteIntentSwitchReplicaRequest) {
                    if (Thread.currentThread().getName().contains(CLEANUP_THREAD_NAME)) {
                        if (failedCleanupAttempts.getAndIncrement() < 3) {
                            retryContexts.stream()
                                    .map(KeyBasedRetryContext::snapshot)
                                    .filter(retryContextSnapshot -> retryContextSnapshot.size() == 1)
                                    .flatMap(retryContextSnapshot -> retryContextSnapshot.values().stream())
                                    .forEach(timeoutState -> timeoutSamples.add(timeoutState.getTimeout()));

                            return true;
                        }
                    }

                    return false;
                }

                return false;
            });
        }

        tx.commitAsync();

        await().timeout(5, TimeUnit.SECONDS).until(() -> failedCleanupAttempts.get() == 3);

        await().timeout(5, TimeUnit.SECONDS).until(() -> pendingWriteIntents(node) == 0);

        assertTrue(timeoutSamples.size() > 1, "Expected at least 2 timeout samples, got: " + timeoutSamples.size());

        for (int i = 1; i < timeoutSamples.size(); i++) {
            assertTrue(timeoutSamples.get(i - 1) < timeoutSamples.get(i), "timeout increasing is expected!");
        }

        await().timeout(5, SECONDS).until(() -> retryContexts.stream()
                .map(KeyBasedRetryContext::snapshot).allMatch(Map::isEmpty));
    }

    /**
     * Verifies that 100 concurrent transactions all have their write intents cleaned up
     * after transient failures.
     *
     * <p>Creates a zone with 25 partitions to distribute transaction load. For each
     * transaction, drops the first {@link WriteIntentSwitchReplicaRequest} on the write
     * intent switch executor thread (keyed by transaction ID) to force exactly one retry
     * per transaction. After all 100 tasks are submitted and the thread pool shuts down,
     * waits for all pending write intents to reach zero and for all per-node retry context
     * entries to be cleaned up.
     *
     * @throws Exception if the thread pool is interrupted during shutdown.
     */
    @Test
    public void testRetryManyConcurrentCleanUps() throws Exception {
        IgniteImpl node = anyNode();

        String zone1Sql = "create zone test_zone_1 (partitions 25, replicas " + REPLICAS
                + ") storage profiles ['" + DEFAULT_STORAGE_PROFILE + "']";
        String table1Sql = "create table test_table_1 (key bigint primary key, val varchar(20)) zone TEST_ZONE_1";

        sql(zone1Sql);
        sql(table1Sql);

        Map<String, AtomicInteger> failedCleanupAttempts = new ConcurrentHashMap<>();

        List<KeyBasedRetryContext> retryContexts = new ArrayList<>();

        for (IgniteImpl n : runningNodesIter()) {
            TxManagerImpl txManager = (TxManagerImpl) n.txManager();
            retryContexts.add(txManager.retryContext());

            n.dropMessages((dest, msg) -> {
                if (msg instanceof WriteIntentSwitchReplicaRequest) {
                    if (Thread.currentThread().getName().contains(CLEANUP_THREAD_NAME)) {
                        String txId = ((WriteIntentSwitchReplicaRequest) msg).txId().toString();

                        return failedCleanupAttempts.computeIfAbsent(txId, key -> new AtomicInteger(0)).getAndIncrement() == 0;
                    }

                    return false;
                }

                return false;
            });
        }

        ExecutorService threadPool = Executors.newFixedThreadPool(10);

        List<? extends Future<?>> futures = IntStream.range(0, 100).mapToObj(i -> threadPool.submit(() -> {
            Transaction tx = node.transactions().begin();
            node.sql().execute(tx, "insert into test_table_1 (key, val) values (?, ?)", i, "val-" + i);

            tx.commitAsync();
        })).collect(toList());

        try {
            futures.forEach(ItTxCleanupFailureTest::getQuietly);
        } finally {
            threadPool.shutdown();
            threadPool.awaitTermination(5, SECONDS);
        }

        await().timeout(5, SECONDS).until(() -> pendingWriteIntents(node) == 0);

        await().timeout(5, SECONDS).until(() -> retryContexts.stream()
                .map(KeyBasedRetryContext::snapshot).allMatch(Map::isEmpty));
    }

    /**
     * Verifies that cleanup retries for transactions targeting different replication groups
     * are tracked independently in each node's retry context, and that independently
     * progressed contexts converge to the same timeout value.
     *
     * <p>Creates two separate single-partition zones with dedicated tables. For each
     * transaction, drops the first two {@link WriteIntentSwitchReplicaRequest} messages
     * arriving on the cleanup thread (keyed by transaction ID), forcing both retry contexts
     * to advance by the same number of steps. On the third attempt for each transaction,
     * captures a snapshot of all retry context entries across all nodes, merging per-node
     * entries by transaction ID — values are identical across nodes for the same transaction
     * since each node applies the same backoff logic independently.
     *
     * <p>After both transactions' write intents are resolved, asserts that:
     * <ul>
     *     <li>exactly two distinct transaction IDs are present in the merged snapshot —
     *         one per transaction, confirming each transaction is tracked under its own key
     *         in every node's retry context;</li>
     *     <li>both entries carry the same timeout value, confirming that independently
     *         progressed per-node retry contexts reach identical timeouts when starting
     *         from the same initial value and advancing the same number of steps.</li>
     * </ul>
     *
     * <p>Finally, asserts that all per-node retry context entries are cleaned up after
     * successful cleanup.
     */
    @Test
    public void testRetryCleanUpsForDifferentZones() {
        IgniteImpl node = anyNode();

        String zone1Sql = "create zone test_zone_1 (partitions 1, replicas " + REPLICAS
                + ") storage profiles ['" + DEFAULT_STORAGE_PROFILE + "']";
        String table1Sql = "create table test_table_1 (key bigint primary key, val varchar(20)) zone TEST_ZONE_1";

        sql(zone1Sql);
        sql(table1Sql);

        String zone2Sql = "create zone test_zone_2 (partitions 1, replicas " + REPLICAS
                + ") storage profiles ['" + DEFAULT_STORAGE_PROFILE + "']";
        String table2Sql = "create table test_table_2 (key bigint primary key, val varchar(20)) zone TEST_ZONE_2";

        sql(zone2Sql);
        sql(table2Sql);

        Map<String, AtomicInteger> failedCleanupAttempts = new ConcurrentHashMap<>();
        Map<String, TimeoutState> timeoutSamples = new ConcurrentHashMap<>();

        List<KeyBasedRetryContext> retryContexts = new ArrayList<>();

        for (IgniteImpl n : runningNodesIter()) {
            TxManagerImpl txManager = (TxManagerImpl) n.txManager();
            retryContexts.add(txManager.retryContext());

            n.dropMessages((dest, msg) -> {
                if (msg instanceof WriteIntentSwitchReplicaRequest) {
                    if (Thread.currentThread().getName().contains(CLEANUP_THREAD_NAME)) {
                        String txId = ((WriteIntentSwitchReplicaRequest) msg).txId().toString();

                        if (failedCleanupAttempts.computeIfAbsent(txId, key -> new AtomicInteger(0)).getAndIncrement() < 2) {
                            return true;
                        }

                        timeoutSamples.putAll(
                                retryContexts.stream()
                                        .map(KeyBasedRetryContext::snapshot)
                                        .filter(snapshot -> !snapshot.isEmpty())
                                        .flatMap(snapshot -> snapshot.entrySet().stream())
                                        .collect(toMap(Entry::getKey, Entry::getValue, (existing, replacement) -> replacement))
                        );
                    }

                    return false;
                }

                return false;
            });
        }

        Transaction tx1 = node.transactions().begin();
        node.sql().execute(tx1, "insert into test_table_1 (key, val) values (1, 'val-1')");

        tx1.commitAsync();

        Transaction tx2 = node.transactions().begin();
        node.sql().execute(tx2, "insert into TEST_TABLE_2 (key, val) values (1, 'val-1')");

        tx2.commitAsync();

        await().timeout(5, SECONDS).until(() -> pendingWriteIntents(node) == 0);

        assertEquals(2, timeoutSamples.size(),
                "Expected timeout state for both transactions, but got: " + timeoutSamples.keySet());

        List<Integer> collectedTimeouts = timeoutSamples.values().stream()
                .map(TimeoutState::getTimeout)
                .distinct()
                .collect(toList());

        assertEquals(1, collectedTimeouts.size(),
                "Expected both transactions to have the same timeout value, but got: " + collectedTimeouts);

        await().timeout(5, SECONDS).until(() -> retryContexts.stream()
                .map(KeyBasedRetryContext::snapshot).allMatch(Map::isEmpty));
    }

    /**
     * Returns the number of pending write intents on the given node by reading the
     * {@link TransactionMetricsSource#METRIC_PENDING_WRITE_INTENTS} metric.
     *
     * <p>Fails the test immediately if the metric is not found, as this indicates
     * a misconfiguration or an unexpected change in metric naming.
     *
     * @param node the node to read the metric from.
     * @return number of pending write intents.
     */
    private static long pendingWriteIntents(IgniteImpl node) {
        Iterable<Metric> metrics = node.metricManager()
                .metricSnapshot()
                .metrics()
                .get(TransactionMetricsSource.SOURCE_NAME);

        for (Metric m : metrics) {
            if (m.name().equals(METRIC_PENDING_WRITE_INTENTS)) {
                return ((LongMetric) m).value();
            }
        }

        fail();

        return -1;
    }

    /**
     * Waits for the given {@link Future} to complete and returns its result.
     *
     * <p>Wraps checked exceptions as {@link AssertionError} so they propagate cleanly
     * through {@link java.util.function.Consumer} lambdas in test code without requiring
     * explicit try-catch blocks.
     *
     * <ul>
     *     <li>{@link ExecutionException} — wraps the cause as an {@link AssertionError},
     *         preserving the original exception for diagnosis.</li>
     *     <li>{@link InterruptedException} — restores the interrupt flag and wraps
     *         as an {@link AssertionError}.</li>
     * </ul>
     *
     * @param <T>    the future's result type.
     * @param future the future to wait for.
     * @return the future's result.
     * @throws AssertionError if the future completed exceptionally or the thread was interrupted.
     */
    private static <T> T getQuietly(Future<T> future) {
        try {
            return future.get();
        } catch (ExecutionException e) {
            throw new AssertionError("Future completed exceptionally", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for future", e);
        }
    }
}
