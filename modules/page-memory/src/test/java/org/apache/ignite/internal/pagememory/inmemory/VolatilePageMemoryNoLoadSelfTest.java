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

package org.apache.ignite.internal.pagememory.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.pagememory.AbstractPageMemoryNoLoadSelfTest;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.configuration.VolatileDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests {@link VolatilePageMemory}.
 */
@ExtendWith(ConfigurationExtension.class)
public class VolatilePageMemoryNoLoadSelfTest extends AbstractPageMemoryNoLoadSelfTest {
    /** {@inheritDoc} */
    @Override
    protected VolatilePageMemory memory() {
        PageIoRegistry ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        return new VolatilePageMemory(
                VolatileDataRegionConfiguration.builder().pageSize(PAGE_SIZE).initSize(MAX_MEMORY_SIZE).maxSize(MAX_MEMORY_SIZE).build(),
                ioRegistry,
                new OffheapReadWriteLock(OffheapReadWriteLock.DEFAULT_CONCURRENCY_LEVEL)
        );
    }

    @Test
    public void testLoadedPagesCount() {
        PageMemory mem = memory();

        mem.start();

        int expPages = MAX_MEMORY_SIZE / mem.systemPageSize();

        try {
            assertThrows(IgniteOutOfMemoryException.class, () -> {
                for (int i = 0; i < expPages * 2; i++) {
                    allocatePage(mem);
                }
            });

            assertEquals(mem.loadedPages(), expPages);
        } finally {

            mem.stop(true);
        }
    }
}
