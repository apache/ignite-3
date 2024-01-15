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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.cache.Cache.Entry;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.cache.IgniteCache;
import org.apache.ignite.cache.CacheTransaction;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.replication.request.ReadOnlySingleRowPkReplicaRequestBuilder;
import org.apache.ignite.internal.table.distributed.replication.request.ReadOnlySingleRowPkReplicaRequestImpl;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.message.TxCleanupMessage;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.network.DefaultMessagingService;
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
    private static final IgniteLogger LOG = Loggers.forClass(ItCacheTest.class);

    private static final int CACHE_TABLE_ID = 2;

    private static final String CACHE_NAME = "test";

    private TableViewInternal testTable;

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

    private static SchemaDescriptor SCHEMA_DESC = new SchemaDescriptor(
            1,
            new Column[]{new Column("key".toUpperCase(), NativeTypes.BYTES, false)},
            new Column[]{
                    new Column("ttl".toUpperCase(), NativeTypes.INT64, false),
                    new Column("value".toUpperCase(), NativeTypes.BYTES, false)
            }
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

        testTable = txTestCluster.startTable(CACHE_NAME, CACHE_TABLE_ID, SCHEMA_DESC);

        log.info("Caches have been started");
    }

    @AfterEach
    public void after() throws Exception {
        txTestCluster.shutdownCluster();
    }

    /**
     * Test basic cache operations.
     */
    @Test
    public void testBasic() {
        try (IgniteCache<Integer, Integer> view = testTable.cacheView(txTestCluster.clientTxManager, null, null, null, null)) {
            view.put(1, 1);

            assertEquals(1, view.get(1));

            assertTrue(view.remove(1));

            assertNull(view.get(1));

            view.put(1, 2);

            assertEquals(2, view.get(1));

            assertTrue(view.remove(1));

            assertNull(view.get(1));
        }
    }

    /**
     * Test explicit cache transaction (without external store).
     */
    @Test
    public void testExplicitTxn() {
        try (IgniteCache<Integer, Integer> view = testTable.cacheView(txTestCluster.clientTxManager, null, null, null, null)) {
            CacheTransaction tx = view.beginTransaction();

            view.put(tx, 1, 1);

            assertEquals(1, view.get(tx, 1));

            assertTrue(view.remove(tx, 1));

            assertNull(view.get(tx, 1));

            view.put(tx, 1, 2);

            assertEquals(2, view.get(tx, 1));

            assertTrue(view.remove(tx, 1));

            assertNull(view.get(tx, 1));

            tx.commit();

            assertNull(view.get(1));
        }
    }

    /**
     * Test basic cache operations.
     */
    @Test
    public void testReadWriteThrough() {
        Map<Integer, Integer> testStore = new HashMap<>();

        TestCacheLoader loader = new TestCacheLoader(testStore);
        TestCacheWriter writer = new TestCacheWriter(testStore);
        try (IgniteCache<Integer, Integer> view = testTable.cacheView(
                txTestCluster.clientTxManager,
                loader,
                writer,
                null,
                null)
        ) {
            assertNull(view.get(1));
            validate(testStore, loader, writer, 0, 1, 0, 0);

            assertNull(view.get(1)); // Repeating get should fetch tombstone and avoid loading from store.
            validate(testStore, loader, writer, 0, 1, 0, 0);

            view.put(1, 1);
            validate(testStore, loader, writer, 1, 1, 1, 0);

            assertEquals(1, view.get(1));
            validate(testStore, loader, writer, 1, 1, 1, 0);

            assertTrue(view.remove(1));
            validate(testStore, loader, writer, 0, 1, 1, 1);

            assertNull(view.get(1));
            validate(testStore, loader, writer, 0, 2, 1, 1);
        }
    }

    /**
     * Test basic cache operations.
     */
    @Test
    public void testReadWriteThroughExplicitTxn() {
        Map<Integer, Integer> testStore = new HashMap<>();

        TestCacheLoader loader = new TestCacheLoader(testStore);
        TestCacheWriter writer = new TestCacheWriter(testStore);
        try (IgniteCache<Integer, Integer> view = testTable.cacheView(
                txTestCluster.clientTxManager,
                loader,
                writer,
                null,
                null)
        ) {
            CacheTransaction tx = view.beginTransaction();

            assertNull(view.get(tx, 1));
            validate(testStore, loader, writer, 0, 1, 0, 0);

            assertNull(view.get(tx, 1)); // Repeating get should fetch tombstone and avoid loading from store.
            validate(testStore, loader, writer, 0, 1, 0, 0);

            view.put(tx, 1, 1);
            validate(testStore, loader, writer, 0, 1, 0, 0);

            assertEquals(1, view.get(tx, 1));
            validate(testStore, loader, writer, 0, 1, 0, 0);

            assertTrue(view.remove(tx, 1));
            validate(testStore, loader, writer, 0, 1, 0, 0);

            assertNull(view.get(tx, 1));
            validate(testStore, loader, writer, 0, 1, 0, 0);

            tx.commit();

            assertNull(view.get(1));
            validate(testStore, loader, writer, 0, 2, 0, 1);
        }
    }

    @Test
    public void testRecovery() throws Exception {
        try (IgniteCache<Integer, Integer> view = testTable.cacheView(
                txTestCluster.clientTxManager,
                null,
                null,
                null,
                null)
        ) {
            view.put(1, 0);

            CacheTransaction tx = view.beginTransaction();

            view.put(tx, 1, 1);
            assertEquals(1, view.get(tx, 1));
        }

        txTestCluster.stopClient(); // Invalidate txn coordinator.
        TableViewInternal client2 = txTestCluster.newTableClient(CACHE_NAME, CACHE_TABLE_ID, SCHEMA_DESC);

        try (IgniteCache<Integer, Integer> view = client2.cacheView(
                txTestCluster.clientTxManager,
                null,
                null,
                null,
                null)
        ) {
            CacheTransaction tx = view.beginTransaction();

            assertEquals(0, view.get(tx, 1));
        }
    }

    @Test
    public void testRecoveryExternal() throws Exception {
        Map<Integer, Integer> testStore = new HashMap<>();

        TestCacheLoader loader = new TestCacheLoader(testStore);
        TestCacheWriter writer = new TestCacheWriter(testStore);

        try (IgniteCache<Integer, Integer> view = testTable.cacheView(
                txTestCluster.clientTxManager,
                loader,
                writer,
                null,
                null)
        ) {
            view.put(1, 0);
            assertEquals(0, view.get(1));
            assertEquals(0, testStore.get(1));

            CacheTransaction tx = view.beginTransaction();

            view.put(tx, 1, 1);
            assertEquals(1, view.get(tx, 1));
        }

        txTestCluster.stopClient(); // Invalidate txn coordinator.
        TableViewInternal client2 = txTestCluster.newTableClient(CACHE_NAME, CACHE_TABLE_ID, SCHEMA_DESC);

        try (IgniteCache<Integer, Integer> view = client2.cacheView(
                txTestCluster.clientTxManager,
                null,
                null,
                null,
                null)
        ) {
            CacheTransaction tx = view.beginTransaction();

            assertEquals(testStore.get(1), view.get(tx, 1));
        }
    }

    @Test
    public void testRecoveryExternal2() throws Exception {
        Map<Integer, Integer> testStore = new HashMap<>();
        testStore.put(1, 0);

        TestCacheLoader loader = new TestCacheLoader(testStore);
        TestCacheWriter writer = new TestCacheWriter(testStore);

        try (IgniteCache<Integer, Integer> view = testTable.cacheView(
                txTestCluster.clientTxManager,
                loader,
                writer,
                null,
                null)
        ) {
            assertEquals(0, view.get(1));
            assertEquals(0, testStore.get(1));

            CacheTransaction tx = view.beginTransaction();

            view.put(tx, 1, 1);
            assertEquals(1, view.get(tx, 1));
        }

        txTestCluster.stopClient(); // Invalidate txn coordinator.
        TableViewInternal client2 = txTestCluster.newTableClient(CACHE_NAME, CACHE_TABLE_ID, SCHEMA_DESC);

        try (IgniteCache<Integer, Integer> view = client2.cacheView(
                txTestCluster.clientTxManager,
                null,
                null,
                null,
                null)
        ) {
            CacheTransaction tx = view.beginTransaction();

            assertEquals(testStore.get(1), view.get(tx, 1));
        }
    }

    @Test
    public void testRecoveryExternalFailAfterExternalCommit() throws Exception {
        Map<Integer, Integer> testStore = new HashMap<>();
        testStore.put(1, 0);

        TestCacheLoader loader = new TestCacheLoader(testStore);
        TestCacheWriter writer = new TestCacheWriter(testStore);

        try (IgniteCache<Integer, Integer> view = testTable.cacheView(
                txTestCluster.clientTxManager,
                loader,
                writer,
                null,
                null)
        ) {
            assertEquals(0, view.get(1));
            assertEquals(0, testStore.get(1));

            CacheTransaction tx = view.beginTransaction();

            view.put(tx, 1, 1);
            assertEquals(1, view.get(tx, 1));

            // Block cleanup
            DefaultMessagingService messagingService = (DefaultMessagingService) txTestCluster.client.messagingService();
            messagingService.dropMessages((s, networkMessage) -> {
                if (networkMessage instanceof TxCleanupMessage) {
                    logger().info("Dropping cleanup request: {}", networkMessage);

                    // Throttle durable cleanup.
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        // Ignored.
                    }

                    return true;
                }
                return false;
            });

            tx.commitAsync();
        }

        txTestCluster.stopClient(); // Invalidate txn coordinator.
        TableViewInternal client2 = txTestCluster.newTableClient(CACHE_NAME, CACHE_TABLE_ID, SCHEMA_DESC);

        try (IgniteCache<Integer, Integer> view = client2.cacheView(
                txTestCluster.clientTxManager,
                loader,
                writer,
                null,
                null)
        ) {
            CacheTransaction tx = view.beginTransaction();

            assertEquals(testStore.get(1), view.get(tx, 1));
        }
    }

    private static void validate(
            Map<Integer, Integer> testStore,
            TestCacheLoader loader,
            TestCacheWriter writer,
            int expSize,
            int expRead,
            int expWrite,
            int expRemove
    ) {
        assertEquals(expSize, testStore.size());
        assertEquals(expRead, loader.getCounter());
        assertEquals(expWrite, writer.getWriteCounter());
        assertEquals(expRemove, writer.getRemoveCounter());
    }

    private static class TestCacheLoader implements CacheLoader<Integer, Integer> {
        private final Map<Integer, Integer> store;
        private long counter;

        public TestCacheLoader(Map<Integer, Integer> store) {
            this.store = store;
        }

        @Override
        public Integer load(Integer key) throws CacheLoaderException {
            counter++;
            return store.get(key);
        }

        @Override
        public Map<Integer, Integer> loadAll(Iterable<? extends Integer> keys) throws CacheLoaderException {
            return null;
        }

        public long getCounter() {
            return counter;
        }
    }

    private static class TestCacheWriter implements CacheWriter<Integer, Integer> {
        private final Map<Integer, Integer> store;
        private long writeCounter;
        private long removeCounter;

        public TestCacheWriter(Map<Integer, Integer> store) {
            this.store = store;
        }

        @Override
        public void write(Entry<? extends Integer, ? extends Integer> entry) throws CacheWriterException {
            writeCounter++;
            store.put(entry.getKey(), entry.getValue());
        }

        @Override
        public void writeAll(Collection<Entry<? extends Integer, ? extends Integer>> entries) throws CacheWriterException {

        }

        @Override
        public void delete(Object key) throws CacheWriterException {
            removeCounter++;
            store.remove(key);
        }

        @Override
        public void deleteAll(Collection<?> keys) throws CacheWriterException {

        }

        public long getWriteCounter() {
            return writeCounter;
        }

        public long getRemoveCounter() {
            return removeCounter;
        }
    }
}
