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

package org.apache.ignite.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.cache.CacheStore;
import org.apache.ignite.cache.CacheStoreSession;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test cache.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItCacheTest extends IgniteAbstractTest {
    private static final String KEY = "KEY";
    private static final String VALUE = "VALUE";

    private static final String CACHE_NAME = "test";
    private static final String CACHE_NAME_2 = "test2";

    private TableViewInternal cache;

    private TableViewInternal cache2;

    protected final TestInfo testInfo;

    //TODO fsync can be turned on again after https://issues.apache.org/jira/browse/IGNITE-20195
    @InjectConfiguration("mock: { fsync: false }")
    private static RaftConfiguration raftConfiguration;

    @InjectConfiguration
    protected static GcConfiguration gcConfig;

    @InjectConfiguration
    protected static TransactionConfiguration txConfiguration;

    private ItTxTestCluster txTestCluster;

    private final HybridTimestampTracker timestampTracker = new HybridTimestampTracker();

    private static final SchemaDescriptor SCHEMA_DESC = new SchemaDescriptor(
            1,
            new Column[]{new Column(KEY, NativeTypes.INT32, false)},
            new Column[]{new Column(VALUE, NativeTypes.INT32, true)}
    );

    /**
     * The constructor.
     *
     * @param testInfo Test info.
     */
    public ItCacheTest(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    @BeforeEach
    public void before() throws Exception {
        txTestCluster = new ItTxTestCluster(
                testInfo,
                raftConfiguration,
                txConfiguration,
                workDir,
                1,
                1,
                true,
                timestampTracker
        );
        txTestCluster.prepareCluster();

        cache = txTestCluster.startCache(CACHE_NAME, 1, SCHEMA_DESC);
        cache2 = txTestCluster.startCache(CACHE_NAME_2, 2, SCHEMA_DESC);

        log.info("Caches have been started");
    }

    @AfterEach
    public void after() throws Exception {
        txTestCluster.shutdownCluster();
    }

    private static Tuple createKey(int val) {
        return Tuple.create().set(KEY, val);
    }

    private static Tuple createValue(int val) {
        return Tuple.create().set(VALUE, val);
    }

    /**
     * Test basic cache operations.
     */
    @Test
    public void testBasic() {
        KeyValueView<Tuple, Tuple> cache = this.cache.keyValueBinaryView(null);

        cache.put(null, createKey(1), createValue(1));

        assertEquals(createValue(1), cache.get(null, createKey(1)));

        assertTrue(cache.remove(null, createKey(1)));

        assertNull(cache.get(null, createKey(1)));

        cache.put(null, createKey(1), createValue(2));

        assertEquals(createValue(2), cache.get(null, createKey(1)));

        assertTrue(cache.remove(null, createKey(1)));

        assertNull(cache.get(null, createKey(1)));
    }

    /**
     * Test basic cache operations.
     */
    @Test
    public void testBasicTxn() {
        Transaction txn = txTestCluster.igniteTransactions().begin(new TransactionOptions().cacheOnly(true));

        KeyValueView<Tuple, Tuple> cache = this.cache.keyValueBinaryView(null);

        cache.put(txn, createKey(1), createValue(1));

        assertEquals(createValue(1), cache.get(txn, createKey(1)));

        assertTrue(cache.remove(txn, createKey(1)));

        assertNull(cache.get(txn, createKey(1)));

        cache.put(txn, createKey(1), createValue(2));

        assertEquals(createValue(2), cache.get(txn, createKey(1)));

        assertTrue(cache.remove(txn, createKey(1)));

        assertNull(cache.get(txn, createKey(1)));

        txn.commit();

        assertNull(cache.get(txn, createKey(1)));
    }

    /**
     * Test basic cache operations.
     */
    @Test
    public void testBasicTxnCrossCache() {
        KeyValueView<Tuple, Tuple> cache = this.cache.keyValueBinaryView(null);
        KeyValueView<Tuple, Tuple> cache2 = this.cache2.keyValueBinaryView(null);

        Transaction txn = txTestCluster.igniteTransactions().begin(new TransactionOptions().cacheOnly(true));

        cache.put(txn, createKey(1), createValue(1));
        cache2.put(txn, createKey(2), createValue(2));

        txn.commit();

        assertEquals(createValue(1), cache.get(null, createKey(1)));
        assertEquals(createValue(2), cache2.get(null, createKey(2)));
    }

    /**
     * Test basic cache operations.
     */
    @Test
    public void testBasicTxnCrossCacheRollback() {
        KeyValueView<Tuple, Tuple> cache = this.cache.keyValueBinaryView(null);
        KeyValueView<Tuple, Tuple> cache2 = this.cache2.keyValueBinaryView(null);

        Transaction txn = txTestCluster.igniteTransactions().begin(new TransactionOptions().cacheOnly(true));

        cache.put(txn, createKey(1), createValue(1));
        cache2.put(txn, createKey(2), createValue(2));

        txn.rollback();

        assertNull(cache.get(null, createKey(1)));
        assertNull(cache2.get(null, createKey(2)));
    }

    /**
     * Test explicit cache transaction rollback.
     */
    @Test
    public void testExplicitTxnRollback() {
        KeyValueView<Tuple, Tuple> cache = this.cache.keyValueBinaryView(null);

        try {
            txTestCluster.igniteTransactions().runInTransaction((Consumer<Transaction>) txn -> {
                cache.put(txn, createKey(1), createValue(1));

                throw new RuntimeException();
            }, new TransactionOptions().cacheOnly(true));

            fail();
        } catch (Exception e) {
            // Expected.
        }

        assertNull(cache.get(null, createKey(1)));
    }

    /**
     * Test basic cache operations.
     */
    @Test
    public void testReadWriteThrough() {
        Map<Tuple, Tuple> testStore = new HashMap<>();

        TestCacheStore store = new TestCacheStore(testStore);

        KeyValueView<Tuple, Tuple> cache = this.cache.keyValueBinaryView(store);

        assertNull(cache.get(null, createKey(1)));
        validate(testStore, store, 0, 1, 0, 0);

        assertNull(cache.get(null, createKey(1))); // Repeating get should fetch tombstone and avoid loading from store.
        validate(testStore, store, 0, 1, 0, 0);

        cache.put(null, createKey(1), createValue(1));
        validate(testStore, store, 1, 1, 1, 0);

        assertEquals(createValue(1), cache.get(null, createKey(1)));
        validate(testStore, store, 1, 1, 1, 0);

        assertTrue(cache.remove(null, createKey(1)));
        validate(testStore, store, 0, 1, 1, 1);

        assertNull(cache.get(null, createKey(1)));
        validate(testStore, store, 0, 2, 1, 1);
    }

//    /**
//     * Test basic cache operations.
//     */
//    @Test
//    public void testReadWriteThroughExplicitTxn() {
//        Map<Integer, Integer> testStore = new HashMap<>();
//
//        TestCacheLoader loader = new TestCacheLoader(testStore);
//        TestCacheWriter writer = new TestCacheWriter(testStore);
//        try (Cache<Integer, Integer> cache = testTable.cache(
//                txTestCluster.clientTxManager,
//                loader,
//                writer,
//                null,
//                null,
//                null)
//        ) {
//            cache.runAtomically(() -> {
//                assertNull(cache.get(1));
//                validate(testStore, loader, writer, 0, 1, 0, 0);
//
//                assertNull(cache.get(1)); // Repeating get should fetch tombstone and avoid loading from store.
//                validate(testStore, loader, writer, 0, 1, 0, 0);
//
//                cache.put(1, 1);
//                validate(testStore, loader, writer, 0, 1, 0, 0);
//
//                assertEquals(1, cache.get(1));
//                validate(testStore, loader, writer, 0, 1, 0, 0);
//
//                assertTrue(cache.remove(1));
//                validate(testStore, loader, writer, 0, 1, 0, 0);
//
//                assertNull(cache.get(1)); // Repeating get should fetch tombstone from dirty cacge.
//                validate(testStore, loader, writer, 0, 1, 0, 0);
//            });
//
//            assertNull(cache.get(1));
//            validate(testStore, loader, writer, 0, 2, 0, 1);
//        }
//    }
//
//    @Test
//    public void testRecovery() throws Exception {
//        CyclicBarrier b = new CyclicBarrier(2);
//        CyclicBarrier b2 = new CyclicBarrier(2);
//
//        Thread t = new Thread(() -> {
//            try (Cache<Integer, Integer> cache = testTable.cache(
//                    txTestCluster.clientTxManager,
//                    null,
//                    null,
//                    null,
//                    null,
//                    null)
//            ) {
//                cache.runAtomically(() -> {
//                    cache.put(1, 0);
//                    cache.put(1, 1);
//                    assertEquals(1, cache.get(1));
//                    try {
//                        b.await();
//                        b2.await();
//                    } catch (Exception e) {
//                        fail();
//                    }
//                });
//            }
//        });
//        t.start();
//
//        b.await();
//
//        txTestCluster.stopClient(); // Invalidate txn coordinator.
//        TableViewInternal client2 = txTestCluster.newTableClient(CACHE_NAME, CACHE_TABLE_ID, SCHEMA_DESC);
//
//        try (Cache<Integer, Integer> cache = client2.cache(
//                txTestCluster.clientTxManager,
//                null,
//                null,
//                null,
//                null,
//                null)
//        ) {
//            int fails = 0;
//
//            while(true) {
//                try {
//                    // Triggers async recovery for abandoned txn.
//                    assertNull(cache.get(1));
//
//                    break;
//                } catch (TransactionException e) {
//                    fails++;
//                    assertEquals(ACQUIRE_LOCK_ERR, e.code());
//                    Thread.sleep(100); // Retry
//                }
//            }
//
//            assertTrue(fails > 0);
//        }
//
//        b2.await();
//
//        t.join();
//    }
//
//    @Test
//    public void testRecoveryExternal() throws Exception {
//        CyclicBarrier b = new CyclicBarrier(2);
//        CyclicBarrier b2 = new CyclicBarrier(2);
//
//        Map<Integer, Integer> testStore = new ConcurrentHashMap<>();
//
//        TestCacheLoader loader = new TestCacheLoader(testStore);
//        TestCacheWriter writer = new TestCacheWriter(testStore);
//
//        Thread t = new Thread(() -> {
//            try (Cache<Integer, Integer> cache = testTable.cache(
//                    txTestCluster.clientTxManager,
//                    loader,
//                    writer,
//                    null,
//                    null,
//                    null)
//            ) {
//                cache.put(1, 0);
//                assertEquals(0, cache.get(1));
//                assertEquals(0, testStore.get(1));
//
//                cache.runAtomically(() -> {
//                    cache.put(1, 1);
//                    assertEquals(1, cache.get(1));
//                    try {
//                        b.await();
//                        b2.await();
//                    } catch (Exception e) {
//                        fail();
//                    }
//                });
//            }
//        });
//        t.start();
//        b.await();
//
//        txTestCluster.stopClient(); // Invalidate txn coordinator.
//        TableViewInternal client2 = txTestCluster.newTableClient(CACHE_NAME, CACHE_TABLE_ID, SCHEMA_DESC);
//
//        try (Cache<Integer, Integer> cache = client2.cache(
//                txTestCluster.clientTxManager,
//                loader,
//                writer,
//                null,
//                null,
//                null)
//        ) {
//            int fails = 0;
//
//            while(true) {
//                try {
//                    // Triggers async recovery for abandoned txn.
//                    Integer cv = cache.get(1);
//                    Integer sv = testStore.get(1);
//                    assertEquals(0, sv);
//                    assertEquals(0, cv);
//
//                    break;
//                } catch (TransactionException e) {
//                    fails++;
//                    assertEquals(ACQUIRE_LOCK_ERR, e.code());
//                    Thread.sleep(100); // Retry
//                }
//            }
//
//            assertTrue(fails > 0);
//        }
//
//        b2.await();
//
//        t.join();
//    }
//
//    @Test
//    public void testRecoveryExternal2() throws Exception {
//        CyclicBarrier b = new CyclicBarrier(2);
//        CyclicBarrier b2 = new CyclicBarrier(2);
//
//        Map<Integer, Integer> testStore = new ConcurrentHashMap<>();
//        testStore.put(1, 0);
//
//        TestCacheLoader loader = new TestCacheLoader(testStore);
//        TestCacheWriter writer = new TestCacheWriter(testStore);
//
//        Thread t = new Thread(() -> {
//            try (Cache<Integer, Integer> cache = testTable.cache(
//                    txTestCluster.clientTxManager,
//                    loader,
//                    writer,
//                    null,
//                    null,
//                    null)
//            ) {
//                assertEquals(0, cache.get(1));
//                assertEquals(0, testStore.get(1));
//
//                cache.runAtomically(() -> {
//                    cache.put(1, 1);
//                    assertEquals(1, cache.get(1));
//
//                    try {
//                        b.await();
//                        b2.await();
//                    } catch (Exception e) {
//                        fail();
//                    }
//                });
//            }
//        });
//        t.start();
//        b.await();
//
//        txTestCluster.stopClient(); // Invalidate txn coordinator.
//        TableViewInternal client2 = txTestCluster.newTableClient(CACHE_NAME, CACHE_TABLE_ID, SCHEMA_DESC);
//
//        try (Cache<Integer, Integer> cache = client2.cache(
//                txTestCluster.clientTxManager,
//                loader,
//                writer,
//                null,
//                null,
//                null)
//        ) {
//            int fails = 0;
//
//            while(true) {
//                try {
//                    // Triggers async recovery for abandoned txn.
//                    Integer cv = cache.get(1);
//                    Integer sv = testStore.get(1);
//                    assertEquals(0, sv);
//                    assertEquals(0, cv);
//
//                    break;
//                } catch (TransactionException e) {
//                    fails++;
//                    assertEquals(ACQUIRE_LOCK_ERR, e.code());
//                    Thread.sleep(100); // Retry
//                }
//            }
//
//            assertTrue(fails > 0);
//        }
//
//        b2.await();
//
//        t.join();
//    }
//
//    @Test
//    public void testRecoveryExternalFailAfterExternalCommit() throws Exception {
//        Map<Integer, Integer> testStore = new ConcurrentHashMap<>();
//        testStore.put(1, 0);
//
//        TestCacheLoader loader = new TestCacheLoader(testStore);
//        TestCacheWriter writer = new TestCacheWriter(testStore);
//
//        CountDownLatch l = new CountDownLatch(1);
//
//        Thread t = new Thread(() -> {
//            try (Cache<Integer, Integer> cache = testTable.cache(
//                    txTestCluster.clientTxManager,
//                    loader,
//                    writer,
//                    null,
//                    null,
//                    null)
//            ) {
//                assertEquals(0, cache.get(1));
//                assertEquals(0, testStore.get(1));
//
//                cache.runAtomically(() -> {
//                    cache.put(1, 1);
//                    assertEquals(1, cache.get(1));
//
//                    // Block cleanup.
//                    DefaultMessagingService messagingService = (DefaultMessagingService) txTestCluster.client.messagingService();
//                    messagingService.dropMessages((s, networkMessage) -> {
//                        if (networkMessage instanceof TxCleanupMessage) {
//                            if (l.getCount() > 0) {
//                                l.countDown();
//                            }
//
//                            logger().info("Dropping cleanup request: {}", networkMessage);
//
//                            // Throttle cleanup retries.
//                            try {
//                                Thread.sleep(1000);
//                            } catch (InterruptedException e) {
//                                // Ignored.
//                            }
//
//                            return true;
//                        }
//
//                        return false;
//                    });
//                });
//            }
//        });
//
//        t.start();
//
//        // Wait until commit was called on external store.
//        assertTrue(l.await(5_000, TimeUnit.MILLISECONDS));
//
//        // Ensure update is applied.
//        assertEquals(1, testStore.get(1));
//
//        txTestCluster.stopClient(); // Invalidate txn coordinator.
//        TableViewInternal client2 = txTestCluster.newTableClient(CACHE_NAME, CACHE_TABLE_ID, SCHEMA_DESC);
//
//        try (Cache<Integer, Integer> cache = client2.cache(
//                txTestCluster.clientTxManager,
//                loader,
//                writer,
//                null,
//                null,
//                null)
//        ) {
//            int fails = 0;
//
//            while(true) {
//                try {
//                    // Triggers async recovery for abandoned txn.
//                    Integer cv = cache.get(1);
//                    Integer sv = testStore.get(1);
//                    assertEquals(1, sv);
//                    assertEquals(1, cv);
//
//                    break;
//                } catch (TransactionException e) {
//                    fails++;
//                    assertEquals(ACQUIRE_LOCK_ERR, e.code());
//                    Thread.sleep(100); // Retry
//                }
//            }
//
//            assertTrue(fails > 0);
//        }
//    }

    private static void validate(
            Map<?, ?> testStore,
            TestCacheStore store,
            int expSize,
            int expRead,
            int expWrite,
            int expRemove
    ) {
        assertEquals(expSize, testStore.size());
        assertEquals(expRead, store.getReadCounter());
        assertEquals(expWrite, store.getWriteCounter());
        assertEquals(expRemove, store.getRemoveCounter());
    }

    private static class TestCacheStore implements CacheStore {
        private final Map<Tuple, Tuple> store;
        private long readCounter;
        private long writeCounter;
        private long removeCounter;

        public TestCacheStore(Map<Tuple, Tuple> store) {
            this.store = store;
        }

        @Override
        public CacheStoreSession beginSession() {
            return null;
        }

        @Override
        public CompletableFuture<Tuple> load(Tuple key) {
            readCounter++;
            return CompletableFuture.completedFuture(store.get(key));
        }

        @Override
        public CompletableFuture<Map<Tuple, Tuple>> loadAll(Iterable<? extends Tuple> keys) throws IgniteException {
            return null;
        }

        @Override
        public void write(@Nullable CacheStoreSession session, Map.Entry<? extends Tuple, ? extends Tuple> entry) {
            writeCounter++;
            store.put(entry.getKey(), entry.getValue());
        }

        @Override
        public void writeAll(@Nullable CacheStoreSession session, Collection<Map.Entry<? extends Tuple, ? extends Tuple>> entries)
                throws IgniteException {
        }

        @Override
        public void delete(@Nullable CacheStoreSession session, Object key) throws IgniteException {
            removeCounter++;
            store.remove(key);
        }

        @Override
        public void deleteAll(@Nullable CacheStoreSession session, Collection<?> keys) throws IgniteException {

        }

        public long getReadCounter() {
            return readCounter;
        }

        public long getWriteCounter() {
            return writeCounter;
        }

        public long getRemoveCounter() {
            return removeCounter;
        }
    }
}
