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

package org.apache.ignite.internal.pagememory.tree.persistence;

import static org.apache.ignite.internal.configuration.ConfigurationTestUtils.fixConfiguration;
import static org.apache.ignite.internal.pagememory.datastructure.DataStructure.rnd;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTestUtils.mockCheckpointTimeoutLock;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runMultiThreaded;
import static org.apache.ignite.internal.util.Constants.MiB;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.LongStream;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.TestPageIoRegistry;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileConfigurationSchema;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.TestPageReadWriteManager;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.tree.AbstractBplusTreePageMemoryTest;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.util.SequencedOffheapReadWriteLock;
import org.apache.ignite.internal.storage.configurations.StorageProfileConfiguration;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Class to test the {@link BplusTree} with {@link PersistentPageMemory}.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItBplusTreePersistentPageMemoryTest extends AbstractBplusTreePageMemoryTest {
    /** Dictates the implementation of {@link OffheapReadWriteLock} that will be used in a test. */
    private static final String USE_SEQUENCED_RW_LOCK = "USE_SEQUENCED_RW_LOCK";

    @InjectConfiguration(
            polymorphicExtensions = { PersistentPageMemoryProfileConfigurationSchema.class },
            value = "mock = {"
            + "engine=aipersist, "
            + "size=" + MAX_MEMORY_SIZE
            + "}"
    )
    private StorageProfileConfiguration storageProfileCfg;

    private OffheapReadWriteLock offheapReadWriteLock;

    /** {@inheritDoc} */
    @Override
    protected PageMemory createPageMemory() {
        TestPageIoRegistry ioRegistry = new TestPageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        offheapReadWriteLock = IgniteSystemProperties.getBoolean(USE_SEQUENCED_RW_LOCK)
                ? new SequencedOffheapReadWriteLock()
                : new OffheapReadWriteLock(128);

        return new PersistentPageMemory(
                (PersistentPageMemoryProfileConfiguration) fixConfiguration(storageProfileCfg),
                ioRegistry,
                LongStream.range(0, CPUS).map(i -> MAX_MEMORY_SIZE / CPUS).toArray(),
                10 * MiB,
                new TestPageReadWriteManager(),
                (page, fullPageId, pageMemoryImpl) -> {
                },
                (fullPageId, buf, tag) -> {
                },
                mockCheckpointTimeoutLock(true),
                () -> null,
                PAGE_SIZE,
                offheapReadWriteLock
        );
    }

    /**
     * Test is based on {@link AbstractBplusTreePageMemoryTest#testMassiveRemove2_true()}, but uses a deterministic execution order in order
     * to achieve a 100% reliable fail rate.
     *
     * @throws Exception If failed.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-23588")
    @Test
    @WithSystemProperty(key = USE_SEQUENCED_RW_LOCK, value = "true")
    @WithSystemProperty(key = BPLUS_TREE_TEST_SEED, value = "1161542256747481")
    public void testMassiveRemoveCorruption() throws Exception {
        SequencedOffheapReadWriteLock offheapReadWriteLock = (SequencedOffheapReadWriteLock) this.offheapReadWriteLock;

        //noinspection AssignmentToStaticFieldFromInstanceMethod
        MAX_PER_PAGE = 2;
        int threads = 16;
        int batch = 500;
        boolean canGetRow = true;

        println("[maxPerPage=" + MAX_PER_PAGE
                + ", threads=" + threads
                + ", batch=" + batch
                + ", canGetRow=" + canGetRow
                + "]"
        );

        int keys = threads * batch;

        TestTree tree = createTestTree(canGetRow);

        // Inverted insertion order, like in original test.
        for (long k = keys - 1; k >= 0; k--) {
            tree.put(k);
        }

        assertEquals(keys, tree.size());
        tree.validateTree();

        AtomicLongArray rmvd = new AtomicLongArray(keys);

        // All the random bits are pre-calculated in advance.
        // It doesn't have to be this way, because there's a critical section in the loop later anyway. But, this is the version of the code
        // with a known seed, so let's leave it as it is right now.
        int[][] rndEx = new int[threads][keys];
        for (int i = 0; i < rndEx.length; i++) {
            for (int j = 0; j < rndEx[i].length; j++) {
                rndEx[i][j] = rnd.nextInt(keys);
            }
        }

        offheapReadWriteLock.startSequencing(() -> rnd.nextInt(threads), threads);

        String threadNamePrefix = "remove";
        runMultiThreaded(() -> {
            // Here we rely on a thread naming convention in "runMultiThreaded" to get thread's ID.
            int threadId = Integer.parseInt(Thread.currentThread().getName().substring(threadNamePrefix.length()));

            offheapReadWriteLock.setCurrentThreadId(threadId);

            try {
                int rndIdx = 0;
                while (true) {
                    int idx = 0;
                    boolean found = false;

                    offheapReadWriteLock.await();
                    try {
                        int shift = rndEx[threadId][rndIdx++];
                        // Since this loop modifies a shared resource ("rmvd"), we must have a critical section here as well.
                        for (int i = 0; i < keys; i++) {
                            idx = (i + shift) % keys;

                            if (rmvd.get(idx) == 0 && rmvd.compareAndSet(idx, 0, 1)) {
                                found = true;

                                break;
                            }
                        }
                    } finally {
                        offheapReadWriteLock.release();
                    }

                    if (!found) {
                        break;
                    }

                    Long key = (long) idx;
                    assertEquals(key, tree.remove(key));

                    if (canGetRow) {
                        rmvdIds.add(key);
                    }
                }
            } finally {
                offheapReadWriteLock.complete();
            }

            return null;
        }, threads, threadNamePrefix);

        offheapReadWriteLock.stopSequencing();

        assertEquals(0, tree.size());
        tree.validateTree();
    }

    /** {@inheritDoc} */
    @Override
    protected long acquiredPages() {
        return ((PersistentPageMemory) pageMem).acquiredPages();
    }

    /** {@inheritDoc} */
    @Override
    protected @Nullable ReuseList createReuseList(
            int grpId,
            int partId,
            PageMemory pageMem,
            long rootId,
            boolean initNew
    ) {
        return null;
    }
}
