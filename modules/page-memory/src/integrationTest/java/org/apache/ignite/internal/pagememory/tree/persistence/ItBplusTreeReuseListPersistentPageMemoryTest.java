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
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.TestPageIoRegistry;
import org.apache.ignite.internal.pagememory.configuration.PersistentDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.persistence.PageHeader;
import org.apache.ignite.internal.pagememory.persistence.PageWriteTarget;
import org.apache.ignite.internal.pagememory.persistence.PartitionDestructionLockManager;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemoryMetricSource;
import org.apache.ignite.internal.pagememory.persistence.TestPageReadWriteManager;
import org.apache.ignite.internal.pagememory.tree.AbstractBplusTreeReusePageMemoryTest;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test with reuse list and {@link PersistentPageMemory}.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItBplusTreeReuseListPersistentPageMemoryTest extends AbstractBplusTreeReusePageMemoryTest {
    @BeforeAll
    static void initLockOffset() {
        lockOffset = PageHeader.PAGE_LOCK_OFFSET;
    }

    /** {@inheritDoc} */
    @Override
    protected PageMemory createPageMemory() throws Exception {
        TestPageIoRegistry ioRegistry = new TestPageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        return new PersistentPageMemory(
                PersistentDataRegionConfiguration.builder().pageSize(PAGE_SIZE).size(MAX_MEMORY_SIZE).build(),
                new PersistentPageMemoryMetricSource("test"),
                ioRegistry,
                LongStream.range(0, CPUS).map(i -> MAX_MEMORY_SIZE / CPUS).toArray(),
                10 * MiB,
                new TestPageReadWriteManager(),
                (fullPageId, buf, tag) -> PageWriteTarget.NONE,
                mockCheckpointTimeoutLock(true),
                wrapLock(new OffheapReadWriteLock(OffheapReadWriteLock.DEFAULT_CONCURRENCY_LEVEL)),
                new PartitionDestructionLockManager()
        );
    }

    /** {@inheritDoc} */
    @Override
    protected long acquiredPages() {
        return ((PersistentPageMemory) pageMem).acquiredPages();
    }
}
