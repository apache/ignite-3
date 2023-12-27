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
import javax.cache.Cache.Entry;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.cache.Cache;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
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
                false,
                timestampTracker
        );
        txTestCluster.prepareCluster();

        testTable = txTestCluster.startCache(CACHE_NAME, CACHE_TABLE_ID);

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
        try (Cache<Integer, Integer> view = testTable.cacheView(txTestCluster.igniteTransactions, null, null, null, null)) {
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
     * Test basic cache operations.
     */
    @Test
    public void testReadWriteThrough() {
        Map<Integer, Integer> testStore = new HashMap<>();

        TestCacheLoader loader = new TestCacheLoader(testStore);
        TestCacheWriter writer = new TestCacheWriter(testStore);
        try (Cache<Integer, Integer> view = testTable.cacheView(
                txTestCluster.igniteTransactions,
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

    private static void validate(
            Map<Integer, Integer> testStore, TestCacheLoader loader,
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
