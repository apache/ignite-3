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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.metrics.TransactionMetricsSource;
import org.apache.ignite.internal.util.retry.KeyBasedRetryContext;
import org.apache.ignite.internal.util.retry.TimeoutState;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

public class ItDurableFailureTest extends ClusterPerTestIntegrationTest {

    private static final String RETRY_THREAD_NAME = "common-scheduler";
    /** Table name. */
    private static final String TABLE_NAME = "test_table";
    private static final int REPLICAS = 3;

    @BeforeEach
    public void setup() {
        String zoneSql = "create zone test_zone (partitions 1, replicas " + REPLICAS
                + ") storage profiles ['" + DEFAULT_STORAGE_PROFILE + "']";
        String tableSql = "create table " + TABLE_NAME + " (key bigint primary key, val varchar(20)) zone TEST_ZONE";

        sql(zoneSql);
        sql(tableSql);
    }

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

        await().timeout(5, SECONDS)
                .until(() -> commitedTransactions(node) == 1);

        assertEquals(1, finishAttempts.get());
    }

    @Test
    public void testDurableFinishRetry() {
        IgniteImpl node = anyNode();
        Transaction tx = node.transactions().begin();
        node.sql().execute(tx, "insert into " + TABLE_NAME + " (key, val) values (1, 'val-1')");

        AtomicInteger failedFinishAttempts = new AtomicInteger();

        List<KeyBasedRetryContext> retryContexts = new ArrayList<>();

        for (IgniteImpl n : runningNodesIter()) {
            retryContexts.add(((TxManagerImpl) n.txManager()).retryContext());

            n.dropMessages((dest, msg) -> {
                if (msg instanceof TxFinishReplicaRequest) {
                    if (failedFinishAttempts.get() == 0) {
                        System.out.println("dropped message: " + Thread.currentThread().getName());
                        // Makes durable finish fail with replication timeout, on the first attempt.
                        return failedFinishAttempts.incrementAndGet() == 1;
                    }

                    if (Thread.currentThread().getName().contains(RETRY_THREAD_NAME)) {
                        System.out.println("successfull retry: " + Thread.currentThread().getName());

                        assertEquals(1, retryContexts.stream()
                                .map(KeyBasedRetryContext::snapshot)
                                .filter(retryContextSnapshot -> retryContextSnapshot.size() == 1)
                                .count());
                    }
                }

                return false;
            });
        }

        tx.commitAsync();

        await().timeout(5, SECONDS)
                .until(() -> commitedTransactions(node) == 1);

        await().timeout(5, SECONDS).until(() -> failedFinishAttempts.get() == 1);

        await().timeout(5, SECONDS).until(() -> retryContexts.stream()
                .map(KeyBasedRetryContext::snapshot).allMatch(Map::isEmpty));
    }

    @Test
    public void testRetryWithGrowingTimeout() {
        IgniteImpl node = anyNode();
        Transaction tx = node.transactions().begin();
        node.sql().execute(tx, "insert into " + TABLE_NAME + " (key, val) values (1, 'val-1')");

        AtomicInteger failedFinishAttempts = new AtomicInteger();

        List<KeyBasedRetryContext> retryContexts = new ArrayList<>();

        List<Integer> timeouts = new CopyOnWriteArrayList<>();

        for (IgniteImpl n : runningNodesIter()) {
            TxManagerImpl txManager = (TxManagerImpl) n.txManager();
            retryContexts.add(txManager.retryContext());

            n.dropMessages((dest, msg) -> {
                if (msg instanceof TxFinishReplicaRequest) {
                    System.out.println("before dropping message: " + Thread.currentThread().getName());

                    if (failedFinishAttempts.getAndIncrement() < 4) {
                        System.out.println("dropped message: " + Thread.currentThread().getName());

                        retryContexts.stream()
                                .map(KeyBasedRetryContext::snapshot)
                                .filter(retryContextSnapshot -> retryContextSnapshot.size() == 1)
                                .flatMap(retryContextSnapshot -> retryContextSnapshot.values().stream())
                                .forEach(timeoutState -> timeouts.add(timeoutState.getTimeout()));

                        return true;
                    }

                    return false;
                }

                return false;
            });
        }

        tx.commitAsync();

        await().timeout(5, SECONDS).until(() -> failedFinishAttempts.get() == 4);

        System.out.println("timeoutsSize: " + timeouts.size());

        await().timeout(5, SECONDS).until(() -> commitedTransactions(node) == 1);

        for (int i = 1; i < timeouts.size(); i++) {
            assertTrue(timeouts.get(i - 1) < timeouts.get(i), "timeout increasing is expected!");
        }

        await().timeout(5, SECONDS).until(() -> retryContexts.stream()
                .map(KeyBasedRetryContext::snapshot).allMatch(Map::isEmpty));
    }

    @RepeatedTest(10)
    public void testRetryManyConcurrentDurableFinishes() throws Exception {
        IgniteImpl node = anyNode();

        String zone1Sql = "create zone test_zone_1 (partitions 25, replicas " + REPLICAS
                + ") storage profiles ['" + DEFAULT_STORAGE_PROFILE + "']";
        String table1Sql = "create table test_table_1 (key bigint primary key, val varchar(20)) zone TEST_ZONE";

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
            futures.forEach(fut -> {
                try {
                    fut.get();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });
        } finally {
            threadPool.shutdown();
            threadPool.awaitTermination(5, SECONDS);
        }

        // Checks that cleanup finally succeeded.
        await().timeout(15, SECONDS).until(() -> commitedTransactions(node) == 100);

        await().timeout(5, SECONDS).until(() -> retryContexts.stream()
                .map(KeyBasedRetryContext::snapshot).allMatch(Map::isEmpty));
    }

    @RepeatedTest(10)
    public void testRetryDurableFinishForDifferentZones() throws Exception {
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
        Map<String, TimeoutState> timeouts = new ConcurrentHashMap<>();

        List<KeyBasedRetryContext> retryContexts = new ArrayList<>();

//        CyclicBarrier barrier = new CyclicBarrier(2);

        for (IgniteImpl n : runningNodesIter()) {
            TxManagerImpl txManager = (TxManagerImpl) n.txManager();
            retryContexts.add(txManager.retryContext());

            n.dropMessages((dest, msg) -> {
                if (msg instanceof TxFinishReplicaRequest) {
                    System.out.println("before dropping message: " + Thread.currentThread().getName());

                    String txId = ((TxFinishReplicaRequest) msg).txId().toString();

                    if (failedFinishAttempts.computeIfAbsent(txId, key -> new AtomicInteger(0)).getAndIncrement() == 0) {
                        System.out.println("message dropped: " + Thread.currentThread().getName());

                        return true;
                    }

                    System.out.println("message will be sent successfully: " + Thread.currentThread().getName());

                    timeouts.putAll(
                            retryContexts.stream()
                                    .map(KeyBasedRetryContext::snapshot)
                                    .filter(snapshot -> !snapshot.isEmpty())
                                    .flatMap(snapshot -> snapshot.entrySet().stream())
                                    .collect(toMap(Entry::getKey, Entry::getValue))
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

//        AtomicReference<Map<String, TimeoutState>> snapshot = new AtomicReference<>();
//
//        await().timeout(10, SECONDS).until(() -> {
//            Optional<Map<String, TimeoutState>> res = retryContexts.stream()
//                    .map(KeyBasedRetryContext::snapshot)
//                    .filter(snapshot -> snapshot.size() == 2)
//                    .findAny();
//
//            res.ifPresent(snapshot -> );
//
//            return res;
//        });

        // Checks that cleanup finally succeeded.
        await().timeout(15, SECONDS).until(() -> commitedTransactions(node) == 2);

        assertEquals(2, timeouts.size());

        Iterator<TimeoutState> it = timeouts.values().iterator();
        assertEquals(it.next().getTimeout(), it.next().getTimeout());

        await().timeout(5, SECONDS).until(() -> retryContexts.stream()
                .map(KeyBasedRetryContext::snapshot).allMatch(Map::isEmpty));
    }

    private static long commitedTransactions(IgniteImpl node) {
        Iterable<Metric> metrics = node.metricManager()
                .metricSnapshot()
                .metrics()
                .get(TransactionMetricsSource.SOURCE_NAME);

        for (Metric m : metrics) {
            if ("TotalCommits".equals(m.name())) {
                return ((LongMetric) m).value();
            }
        }

        fail();

        return -1;
    }
}
