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

import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointTestUtils.mockCheckpointTimeoutLock;
import static org.apache.ignite.internal.util.Constants.MiB;

import java.util.stream.LongStream;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.TestPageIoRegistry;
import org.apache.ignite.internal.pagememory.configuration.PersistentDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.persistence.PageHeader;
import org.apache.ignite.internal.pagememory.persistence.PartitionDestructionLockManager;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemoryMetricSource;
import org.apache.ignite.internal.pagememory.persistence.TestPageReadWriteManager;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.tree.AbstractBplusTreePageMemoryTest;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.util.SequencedOffheapReadWriteLock;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Class to test the {@link BplusTree} with {@link PersistentPageMemory}.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItBplusTreePersistentPageMemoryTest extends AbstractBplusTreePageMemoryTest {
    /** Dictates the implementation of {@link OffheapReadWriteLock} that will be used in a test. */
    private static final String USE_SEQUENCED_RW_LOCK = "USE_SEQUENCED_RW_LOCK";

    private OffheapReadWriteLock offheapReadWriteLock;

    @BeforeAll
    static void initLockOffset() {
        lockOffset = PageHeader.PAGE_LOCK_OFFSET;
    }

    /** {@inheritDoc} */
    @Override
    protected PageMemory createPageMemory() {
        TestPageIoRegistry ioRegistry = new TestPageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        offheapReadWriteLock = IgniteSystemProperties.getBoolean(USE_SEQUENCED_RW_LOCK)
                ? new SequencedOffheapReadWriteLock()
                : new OffheapReadWriteLock(OffheapReadWriteLock.DEFAULT_CONCURRENCY_LEVEL);

        return new PersistentPageMemory(
                PersistentDataRegionConfiguration.builder().pageSize(PAGE_SIZE).size(MAX_MEMORY_SIZE).build(),
                new PersistentPageMemoryMetricSource("test"),
                ioRegistry,
                LongStream.range(0, CPUS).map(i -> MAX_MEMORY_SIZE / CPUS).toArray(),
                10 * MiB,
                new TestPageReadWriteManager(),
                (fullPageId, buf, tag) -> {
                },
                mockCheckpointTimeoutLock(true),
                wrapLock(offheapReadWriteLock),
                new PartitionDestructionLockManager()
        );
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
