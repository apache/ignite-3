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
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.tx.metrics.TransactionMetricsSource.METRIC_PENDING_WRITE_INTENTS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicaRequest;
import org.apache.ignite.internal.tx.metrics.TransactionMetricsSource;
import org.apache.ignite.internal.util.retry.KeyBasedRetryContext;
import org.apache.ignite.internal.util.retry.TimeoutState;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for transaction cleanup failure.
 */
public class ItTxCleanupFailureTest extends ClusterPerTestIntegrationTest {
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

        // Cleanup succeeded on first attempt — no retries should have happened
        assertEquals(1, cleanupAttempts.get());
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
                    System.out.println("test: " + Thread.currentThread().getName());

                    if (failedFinishAttempts.get() == 0) {
                        // Makes cleanup fail on write intent switch attempt with replication timeout, on the first attempt.
                        return failedFinishAttempts.incrementAndGet() == 1;
                    }

                    if (Thread.currentThread().getName().contains("common-scheduler")) {
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

//        await().timeout(5, TimeUnit.SECONDS)
//                .until(() -> pendingWriteIntents(node) == 0);

        await().timeout(5, TimeUnit.SECONDS).until(() -> failedFinishAttempts.get() == 1);

        await().timeout(5, TimeUnit.SECONDS).until(() -> retryContexts.stream()
                .map(KeyBasedRetryContext::snapshot).allMatch(Map::isEmpty));
    }

//    @Test
//    public void testRetry() {
//        IgniteImpl node = anyNode();
//        Transaction tx = node.transactions().begin();
//        node.sql().execute(tx, "insert into " + TABLE_NAME + " (key, val) values (1, 'val-1')");
//
//        AtomicInteger failedCleanupAttempts = new AtomicInteger();
//
//        for (IgniteImpl n : runningNodesIter()) {
//            n.dropMessages((dest, msg) -> {
//                if (msg instanceof WriteIntentSwitchReplicaRequest && failedCleanupAttempts.get() == 0) {
//                    // Makes cleanup fail on write intent switch attempt with replication timeout, on the first attempt.
//                    return failedCleanupAttempts.incrementAndGet() == 1;
//                }
//
//                return false;
//            });
//        }
//
//        tx.commitAsync();
//
//        await().timeout(5, TimeUnit.SECONDS).until(() -> failedCleanupAttempts.get() == 1);
//
//        // Checks that cleanup finally succeeded.
//        await().timeout(5, TimeUnit.SECONDS).until(() -> pendingWriteIntents(node) == 0);
//    }

    @Test
    public void testRetry() {
        IgniteImpl node = anyNode();
        Transaction tx = node.transactions().begin();
        node.sql().execute(tx, "insert into " + TABLE_NAME + " (key, val) values (1, 'val-1')");

        AtomicInteger failedCleanupAttempts = new AtomicInteger();

        List<KeyBasedRetryContext> retryContexts = new ArrayList<>();

        for (IgniteImpl n : runningNodesIter()) {
            retryContexts.add(((TxManagerImpl) n.txManager()).retryContext());

            n.dropMessages((dest, msg) -> {
                if (msg instanceof WriteIntentSwitchReplicaRequest) {
                    if (failedCleanupAttempts.get() == 0) {

                        // Makes cleanup fail on write intent switch attempt with replication timeout, on the first attempt.
                        return failedCleanupAttempts.incrementAndGet() == 1;
                    }

                    if (Thread.currentThread().getName().contains("tx-async-write-intent")) {
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

        await().timeout(5, TimeUnit.SECONDS).until(() -> failedCleanupAttempts.get() == 1);

        // Checks that cleanup finally succeeded.
        await().timeout(5, TimeUnit.SECONDS).until(() -> pendingWriteIntents(node) == 0);

        await().timeout(5, TimeUnit.SECONDS).until(() -> retryContexts.stream()
                .map(KeyBasedRetryContext::snapshot).allMatch(Map::isEmpty));
    }

    @Test
    public void testRetryWithGrowingTimeout() {
        IgniteImpl node = anyNode();
        Transaction tx = node.transactions().begin();
        node.sql().execute(tx, "insert into " + TABLE_NAME + " (key, val) values (1, 'val-1')");

        AtomicInteger failedCleanupAttempts = new AtomicInteger();

        List<KeyBasedRetryContext> retryContexts = new ArrayList<>();

        List<Integer> timeouts = new CopyOnWriteArrayList<>();

        for (IgniteImpl n : runningNodesIter()) {
            TxManagerImpl txManager = (TxManagerImpl) n.txManager();
            retryContexts.add(txManager.retryContext());
            String threadPrefix = ((IgniteThreadFactory) ((ThreadPoolExecutor) txManager.writeIntentSwitchExecutor()).getThreadFactory()).prefix();

            n.dropMessages((dest, msg) -> {
                if (msg instanceof WriteIntentSwitchReplicaRequest) {
                    if (Thread.currentThread().getName().contains(threadPrefix)) {
                        if (failedCleanupAttempts.getAndIncrement() < 4) {
                            retryContexts.stream()
                                    .map(KeyBasedRetryContext::snapshot)
                                    .filter(retryContextSnapshot -> retryContextSnapshot.size() == 1)
                                    .flatMap(retryContextSnapshot -> retryContextSnapshot.values().stream())
                                    .forEach(timeoutState -> timeouts.add(timeoutState.getTimeout()));

                            return true;
                        }
                    }

                    return false;
                }

                return false;
            });
        }

        tx.commitAsync();

        await().timeout(5, TimeUnit.SECONDS).until(() -> failedCleanupAttempts.get() == 4);

        // Checks that cleanup finally succeeded.
        await().timeout(5, TimeUnit.SECONDS).until(() -> pendingWriteIntents(node) == 0);

        for (int i = 1; i < timeouts.size(); i++) {
            assertTrue(timeouts.get(i - 1) < timeouts.get(i), "timeout increasing is expected!");
        }

        await().timeout(5, TimeUnit.SECONDS).until(() -> retryContexts.stream()
                .map(KeyBasedRetryContext::snapshot).allMatch(Map::isEmpty));
    }

//    @Test
//    public void testRetryWithHavingDifferentTimeoutsForDifferentCommitPartitions() {
//        IgniteImpl node = anyNode();
//
//        Map<String, Integer> failedCleanupAttempts = new ConcurrentHashMap<>();
//
//        List<KeyBasedRetryContext> retryContexts = new ArrayList<>();
//
//        List<Integer> timeouts = new CopyOnWriteArrayList<>();
//
//        for (IgniteImpl n : runningNodesIter()) {
//            TxManagerImpl txManager = (TxManagerImpl) n.txManager();
//            retryContexts.add(txManager.retryContext());
//            String threadPrefix = ((IgniteThreadFactory) ((ThreadPoolExecutor) txManager.writeIntentSwitchExecutor()).getThreadFactory()).prefix();
//
//            n.dropMessages((dest, msg) -> {
//                if (msg instanceof WriteIntentSwitchReplicaRequest) {
//                    if (Thread.currentThread().getName().contains(threadPrefix)) {
//                        String tablesKey = ((WriteIntentSwitchReplicaRequest) msg).tableIds().stream()
//                                .map(String::valueOf)
//                                .collect(joining("_"));
//
//                        if (failedCleanupAttempts.computeIfAbsent(tablesKey, k -> 0) == 0) {
//                            retryContexts.stream()
//                                    .map(KeyBasedRetryContext::snapshot)
//                                    .filter(retryContextSnapshot -> retryContextSnapshot.size() == 1)
//                                    .flatMap(retryContextSnapshot -> retryContextSnapshot.values().stream())
//                                    .forEach(timeoutState -> timeouts.add(timeoutState.getTimeout()));
//
//                            failedCleanupAttempts.compute(tablesKey, )
//
//                            return;
//                        }
//                    }
//
//                    return false;
//                }
//
//                return false;
//            });
//        }
//
//        Transaction tx1 = node.transactions().begin();
//        node.sql().execute(tx1, "insert into " + TABLE_NAME_1 + " (key, val) values (1, 'val-1')");
//
//        tx1.commitAsync();
//
//        Transaction tx2 = node.transactions().begin();
//        node.sql().execute(tx2, "insert into " + TABLE_NAME_2 + " (key, val) values (2, 'val-2')");
//
//        tx1.commitAsync();
//
//        await().timeout(5, TimeUnit.SECONDS).until(() -> failedCleanupAttempts.get() == 4);
//
//        // Checks that cleanup finally succeeded.
//        await().timeout(5, TimeUnit.SECONDS).until(() -> pendingWriteIntents(node) == 0);
//
//        for (int i = 1; i < timeouts.size(); i++) {
//            assertTrue(timeouts.get(i - 1) < timeouts.get(i), "timeout increasing is expected!");
//        }
//
//        await().timeout(5, TimeUnit.SECONDS).until(() -> retryContexts.stream()
//                .map(KeyBasedRetryContext::snapshot).allMatch(Map::isEmpty));
//    }

    @Test
    public void testRetryManyConcurrentCleanUps() throws Exception {
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
            String threadPrefix = ((IgniteThreadFactory) ((ThreadPoolExecutor) txManager.writeIntentSwitchExecutor()).getThreadFactory()).prefix();

            n.dropMessages((dest, msg) -> {
                if (msg instanceof WriteIntentSwitchReplicaRequest) {
                    if (Thread.currentThread().getName().contains(threadPrefix)) {
                        String txId = ((WriteIntentSwitchReplicaRequest) msg).txId().toString();

                        if (failedCleanupAttempts.computeIfAbsent(txId, key -> new AtomicInteger(0)).getAndIncrement() == 0) {
                            return true;
                        }
                    }

                    return false;
                }

                return false;
            });
        }

        ExecutorService threadPool = Executors.newFixedThreadPool(10);

        List<? extends Future<?>> futures = IntStream.range(0, 200).mapToObj(i -> threadPool.submit(() -> {
            Transaction tx = node.transactions().begin();
            node.sql().execute(tx, "insert into test_table_1 (key, val) values (?, ?)", i, "val-" + i);

            tx.commitAsync();
        })).collect(Collectors.toList());

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
        await().timeout(5, SECONDS).until(() -> pendingWriteIntents(node) == 0);

        await().timeout(5, SECONDS).until(() -> retryContexts.stream()
                .map(KeyBasedRetryContext::snapshot).allMatch(Map::isEmpty));
    }

    @Test
    public void testRetryCleanUpsForDifferentZones() throws Exception {
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

        List<KeyBasedRetryContext> retryContexts = new ArrayList<>();

//        CountDownLatch latch = new CountDownLatch(2);
        CyclicBarrier barrier = new CyclicBarrier(2);

        for (IgniteImpl n : runningNodesIter()) {
            TxManagerImpl txManager = (TxManagerImpl) n.txManager();
            retryContexts.add(txManager.retryContext());
            String threadPrefix = ((IgniteThreadFactory) ((ThreadPoolExecutor) txManager.writeIntentSwitchExecutor()).getThreadFactory()).prefix();

            n.dropMessages((dest, msg) -> {
                if (msg instanceof WriteIntentSwitchReplicaRequest) {
                    if (Thread.currentThread().getName().contains(threadPrefix)) {
                        String txId = ((WriteIntentSwitchReplicaRequest) msg).txId().toString();

                        if (failedCleanupAttempts.computeIfAbsent(txId, key -> new AtomicInteger(0)).getAndIncrement() == 0) {
                            return true;
                        }

                        try {
                            barrier.await(5, SECONDS);
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                        Map<String, TimeoutState> globalRetryContextSnapshot = retryContexts.stream()
                                .map(KeyBasedRetryContext::snapshot)
                                .filter(snapshot -> !snapshot.isEmpty())
                                .flatMap(snapshot -> snapshot.entrySet().stream())
                                .collect(toMap(
                                        Entry::getKey,
                                        Entry::getValue
                                ));
                        assertEquals(2, globalRetryContextSnapshot.size());

                        Iterator<TimeoutState> it = globalRetryContextSnapshot.values().iterator();
                        assertEquals(it.next().getTimeout(), it.next().getTimeout());
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

        // Checks that cleanup finally succeeded.
        await().timeout(5, SECONDS).until(() -> pendingWriteIntents(node) == 0);

        await().timeout(5, SECONDS).until(() -> retryContexts.stream()
                .map(KeyBasedRetryContext::snapshot).allMatch(Map::isEmpty));
    }

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
}
