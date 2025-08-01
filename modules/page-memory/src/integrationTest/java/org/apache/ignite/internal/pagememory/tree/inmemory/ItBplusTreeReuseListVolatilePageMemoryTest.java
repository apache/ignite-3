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

package org.apache.ignite.internal.pagememory.tree.inmemory;

import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.TestPageIoRegistry;
import org.apache.ignite.internal.pagememory.configuration.VolatileDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.inmemory.VolatilePageMemory;
import org.apache.ignite.internal.pagememory.tree.AbstractBplusTreeReusePageMemoryTest;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test with reuse list and {@link VolatilePageMemory}.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItBplusTreeReuseListVolatilePageMemoryTest extends AbstractBplusTreeReusePageMemoryTest {
    @BeforeAll
    static void initLockOffset() {
        lockOffset = VolatilePageMemory.LOCK_OFFSET;
    }

    /** {@inheritDoc} */
    @Override
    protected PageMemory createPageMemory() {
        TestPageIoRegistry ioRegistry = new TestPageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        return new VolatilePageMemory(
                VolatileDataRegionConfiguration.builder().pageSize(PAGE_SIZE).initSize(MAX_MEMORY_SIZE).maxSize(MAX_MEMORY_SIZE).build(),
                ioRegistry,
                wrapLock(new OffheapReadWriteLock(OffheapReadWriteLock.DEFAULT_CONCURRENCY_LEVEL))
        );
    }

    /** {@inheritDoc} */
    @Override
    protected long acquiredPages() {
        return ((VolatilePageMemory) pageMem).acquiredPages();
    }
}
