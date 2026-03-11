package org.apache.ignite.tx.distributed;

import static java.util.concurrent.Executors.*;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
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
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.metrics.TransactionMetricsSource;
import org.apache.ignite.internal.util.retry.KeyBasedRetryContext;
import org.apache.ignite.internal.util.retry.TimeoutState;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


/**
 * Integration tests verifying the retry behavior of durable transaction finish
 * ({@link TxFinishReplicaRequest}) in a 3-node Apache Ignite cluster.
 *
 * <p>Each test drops or intercepts {@link TxFinishReplicaRequest} messages on all nodes
 * to simulate network failures, then verifies that:
 * <ul>
 *     <li>the finish operation is retried as expected;</li>
 *     <li>retry timeouts grow exponentially between attempts;</li>
 *     <li>the retry context is cleaned up after success;</li>
 *     <li>no unnecessary retries occur when the first attempt succeeds.</li>
 * </ul>
 *
 * <p>Each test creates a single-partition, 3-replica zone and table in {@code @BeforeEach}.
 * Tests that require additional zones/tables create them locally.
 */
public class ItDurableFinishFailureTest extends ClusterPerTestIntegrationTest {
    /**
     * Thread name fragment identifying the scheduler thread that sends retry attempts.
     * Used to distinguish retried messages from the original commit attempt in message interceptors.
     */
    private static final String RETRY_THREAD_NAME = "common-scheduler";

    /** Metric name for the total number of committed transactions, used to verify commit completion. */
    private static final String TOTAL_COMMITED_TRANSACTIONS_METRIC_NAME = "TotalCommits";

    /** Name of the default test table created in {@code @BeforeEach}. */
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
     * Verifies that no retry occurs when the durable finish succeeds on the first attempt.
     *
     * <p>Installs a message interceptor that counts {@link TxFinishReplicaRequest} messages
     * without dropping any of them. After the commit completes, asserts that exactly one
     * finish message was sent across all nodes.
     */
    @Test
    public void testNoRetryOnSuccessfulFinish() {
        IgniteImpl node = anyNode();
        Transaction tx = node.transactions().begin();
        node.sql().execute(tx, "insert into " + TABLE_NAME + " (key, val) values (1, 'val-1')");

        AtomicInteger finishAttempts = new AtomicInteger();

        for (IgniteImpl n : runningNodesIter()) {
            n.dropMessages((dest, msg) -> {
                if (msg instanceof TxFinishReplicaRequest) {
                    finishAttempts.incrementAndGet();
                }

                return false;
            });
        }

        tx.commitAsync();

        await().timeout(5, SECONDS).until(() -> commitedTransactions(node) == 1);

        await().timeout(1, SECONDS).until(() -> finishAttempts.get() >= 1);

        assertEquals(1, finishAttempts.get());
    }

    /**
     * Verifies that the durable finish is retried after a single failure.
     *
     * <p>Drops the first {@link TxFinishReplicaRequest} on all nodes to simulate a transient
     * network failure. On the subsequent retry, captures a snapshot of the retry context
     * across all nodes and asserts that exactly one node has an active retry entry for the
     * transaction. After the commit completes, asserts that the retry context is fully cleaned up.
     */
    @Test
    public void testDurableFinishRetry() {
        IgniteImpl node = anyNode();
        Transaction tx = node.transactions().begin();
        node.sql().execute(tx, "insert into " + TABLE_NAME + " (key, val) values (1, 'val-1')");

        AtomicInteger failedFinishAttempts = new AtomicInteger();

        List<KeyBasedRetryContext> retryContexts = new ArrayList<>();

        AtomicLong expectedSizeOfRetryContext = new AtomicLong(0);

        for (IgniteImpl n : runningNodesIter()) {
            retryContexts.add(((TxManagerImpl) n.txManager()).retryContext());

            n.dropMessages((dest, msg) -> {
                if (msg instanceof TxFinishReplicaRequest) {
                    if (failedFinishAttempts.get() == 0) {
                        // Makes durable finish fail with replication timeout, on the first attempt.
                        return failedFinishAttempts.incrementAndGet() == 1;
                    }

                    expectedSizeOfRetryContext.compareAndSet(0, retryContexts.stream()
                            .map(KeyBasedRetryContext::snapshot)
                            .filter(retryContextSnapshot -> retryContextSnapshot.size() == 1)
                            .count());
                }

                return false;
            });
        }

        tx.commitAsync();

        await().timeout(5, SECONDS).until(() -> failedFinishAttempts.get() == 1);

        await().timeout(5, SECONDS).until(() -> commitedTransactions(node) == 1);

        assertEquals(1, expectedSizeOfRetryContext.get());

        await().timeout(5, SECONDS).until(() -> retryContexts.stream()
                .map(KeyBasedRetryContext::snapshot).allMatch(Map::isEmpty));
    }

    /**
     * Verifies that retry timeouts grow monotonically between consecutive retry attempts.
     *
     * <p>Drops the first 3 {@link TxFinishReplicaRequest} messages. For each retry attempt
     * arriving on the scheduler thread, captures the current timeout from the retry context
     * snapshot. After all drops and the final successful commit, asserts that the captured
     * timeout sequence is strictly increasing, confirming exponential backoff behavior.
     *
     * <p>Timeout samples are captured only on the retry scheduler thread
     * (identified by {@link #RETRY_THREAD_NAME}) to ensure we observe post-advancement values
     * rather than the stale pre-drop state.
     */
    @Test
    public void testRetryWithGrowingTimeout() {
        IgniteImpl node = anyNode();
        Transaction tx = node.transactions().begin();
        node.sql().execute(tx, "insert into " + TABLE_NAME + " (key, val) values (1, 'val-1')");

        AtomicInteger failedFinishAttempts = new AtomicInteger();

        List<KeyBasedRetryContext> retryContexts = new ArrayList<>();

        List<Integer> timeoutSamples = new CopyOnWriteArrayList<>();

        for (IgniteImpl n : runningNodesIter()) {
            TxManagerImpl txManager = (TxManagerImpl) n.txManager();
            retryContexts.add(txManager.retryContext());

            n.dropMessages((dest, msg) -> {
                if (msg instanceof TxFinishReplicaRequest) {
                    if (Thread.currentThread().getName().contains(RETRY_THREAD_NAME)) {
                        retryContexts.stream()
                                .map(KeyBasedRetryContext::snapshot)
                                .filter(retryContextSnapshot -> retryContextSnapshot.size() == 1)
                                .flatMap(retryContextSnapshot -> retryContextSnapshot.values().stream())
                                .forEach(timeoutState -> timeoutSamples.add(timeoutState.getTimeout()));
                    }

                    if (failedFinishAttempts.getAndIncrement() < 3) {
                        return true;
                    }

                    return false;
                }

                return false;
            });
        }

        tx.commitAsync();

        await().timeout(5, SECONDS).until(() -> failedFinishAttempts.get() == 3);

        await().timeout(5, SECONDS).until(() -> commitedTransactions(node) == 1);

        assertTrue(timeoutSamples.size() > 1, "Expected at least 2 timeout samples, got: " + timeoutSamples.size());

        for (int i = 1; i < timeoutSamples.size(); i++) {
            assertTrue(timeoutSamples.get(i - 1) < timeoutSamples.get(i), "timeout increasing is expected!");
        }

        await().timeout(5, SECONDS).until(() -> retryContexts.stream()
                .map(KeyBasedRetryContext::snapshot).allMatch(Map::isEmpty));
    }

    /**
     * Verifies that 100 concurrent transactions are all eventually committed after transient failures.
     *
     * <p>Creates a zone with 25 partitions to distribute load. For each transaction, drops
     * the first {@link TxFinishReplicaRequest} (keyed by transaction ID) to force one retry per
     * transaction. After all tasks are submitted and the thread pool shuts down, waits for all
     * 100 transactions to complete and for all retry context entries to be cleaned up.
     *
     * @throws Exception if the thread pool is interrupted during shutdown.
     */
    @Test
    public void testRetryManyConcurrentDurableFinishes() throws Exception {
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
                if (msg instanceof TxFinishReplicaRequest) {
                    String txId = ((TxFinishReplicaRequest) msg).txId().toString();

                    if (failedCleanupAttempts.computeIfAbsent(txId, key -> new AtomicInteger(0)).getAndIncrement() == 0) {
                        return true;
                    }
                }

                return false;
            });
        }

        ExecutorService threadPool = newFixedThreadPool(10);

        List<? extends Future<?>> futures = IntStream.range(0, 100).mapToObj(i -> threadPool.submit(() -> {
            Transaction tx = node.transactions().begin();
            node.sql().execute(tx, "insert into test_table_1 (key, val) values (?, ?)", i, "val-" + i);

            tx.commitAsync();
        })).collect(toList());

        try {
            futures.forEach(ItDurableFinishFailureTest::getQuietly);
        } finally {
            threadPool.shutdown();
            threadPool.awaitTermination(5, SECONDS);
        }

        await().timeout(15, SECONDS).until(() -> commitedTransactions(node) == 100);

        await().timeout(5, SECONDS).until(() -> retryContexts.stream()
                .map(KeyBasedRetryContext::snapshot).allMatch(Map::isEmpty));
    }

    /**
     * Verifies that transactions targeting different replication groups are tracked
     * independently in the retry context and converge to the same retry timeout value.
     *
     * <p>Creates two separate single-partition zones with dedicated tables. Drops the first
     * two {@link TxFinishReplicaRequest} attempts for each transaction (keyed by transaction ID)
     * to advance both retry contexts by the same number of steps. On the third attempt, captures
     * a snapshot of all retry context entries across all nodes.
     *
     * <p>After both transactions commit, asserts that:
     * <ul>
     *     <li>exactly two distinct entries are present in the captured snapshot — one per transaction,
     *         confirming that each transaction is tracked under its own key;</li>
     *     <li>both entries carry the same timeout value, confirming that independently progressed
     *         retry contexts reach identical timeouts when starting from the same initial value
     *         and advancing the same number of steps.</li>
     * </ul>
     *
     * <p>Finally, asserts that both retry context entries are cleaned up after successful commit.
     */
    @Test
    public void testRetryDurableFinishForDifferentZones() {
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

        Map<String, AtomicInteger> failedFinishAttempts = new ConcurrentHashMap<>();
        Map<String, TimeoutState> timeoutSamples = new ConcurrentHashMap<>();

        List<KeyBasedRetryContext> retryContexts = new ArrayList<>();

        for (IgniteImpl n : runningNodesIter()) {
            TxManagerImpl txManager = (TxManagerImpl) n.txManager();
            retryContexts.add(txManager.retryContext());

            n.dropMessages((dest, msg) -> {
                if (msg instanceof TxFinishReplicaRequest) {
                    String txId = ((TxFinishReplicaRequest) msg).txId().toString();

                    if (failedFinishAttempts.computeIfAbsent(txId, key -> new AtomicInteger(0)).getAndIncrement() < 2) {
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
            });
        }

        Transaction tx1 = node.transactions().begin();
        node.sql().execute(tx1, "insert into test_table_1 (key, val) values (1, 'val-1')");

        tx1.commitAsync();

        Transaction tx2 = node.transactions().begin();
        node.sql().execute(tx2, "insert into TEST_TABLE_2 (key, val) values (1, 'val-1')");

        tx2.commitAsync();

        await().timeout(15, SECONDS).until(() -> commitedTransactions(node) == 2);

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
     * Returns the total number of committed transactions on the given node by reading
     * the {@value #TOTAL_COMMITED_TRANSACTIONS_METRIC_NAME} metric from
     * {@link TransactionMetricsSource}.
     *
     * <p>Fails the test immediately if the metric is not found, as this indicates
     * a misconfiguration or an unexpected change in metric naming.
     *
     * @param node the node to read the metric from.
     * @return total number of committed transactions.
     */
    private static long commitedTransactions(IgniteImpl node) {
        Iterable<Metric> metrics = node.metricManager()
                .metricSnapshot()
                .metrics()
                .get(TransactionMetricsSource.SOURCE_NAME);

        for (Metric m : metrics) {
            if (TOTAL_COMMITED_TRANSACTIONS_METRIC_NAME.equals(m.name())) {
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
