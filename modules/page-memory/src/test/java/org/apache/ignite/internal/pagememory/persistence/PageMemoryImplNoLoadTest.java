/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.pagememory.persistence;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.pagememory.persistence.PageMemoryImpl.PAGE_OVERHEAD;
import static org.apache.ignite.internal.util.Constants.MiB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.stream.LongStream;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.impl.PageMemoryNoLoadSelfTest;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.mem.unsafe.UnsafeMemoryProvider;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link PageMemoryImpl}.
 */
public class PageMemoryImplNoLoadTest extends PageMemoryNoLoadSelfTest {
    /** {@inheritDoc} */
    @Override
    protected PageMemory memory() {
        return memory(LongStream.range(0, 10).map(i -> 5 * MiB).toArray());
    }

    protected PageMemoryImpl memory(long[] sizes) {
        PageIoRegistry ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        return new PageMemoryImpl(
                new UnsafeMemoryProvider(null),
                dataRegionCfg,
                ioRegistry,
                sizes,
                new TestPageReadWriteManager(),
                (page, fullPageId, pageMemoryEx) -> {
                },
                (fullPageId, buf, tag) -> {
                }
        );
    }

    /** {@inheritDoc} */
    @Test
    @Override
    public void testPageHandleDeallocation() {
        // No-op.
    }

    @Test
    void testDirtyPages() throws Exception {
        PageMemoryImpl memory = (PageMemoryImpl) memory();

        memory.start();

        try {
            Set<FullPageId> dirtyPages = Set.of(allocatePage(memory), allocatePage(memory));

            assertThat(memory.dirtyPages(), equalTo(dirtyPages));

            // TODO: IGNITE-16984 After the checkpoint check that there are no dirty pages
        } finally {
            memory.stop(true);
        }
    }

    @Test
    void testSafeToUpdate() throws Exception {
        long systemPageSize = PAGE_SIZE + PAGE_OVERHEAD;

        dataRegionCfg
                .change(c -> c.changeInitSize(128 * systemPageSize).changeMaxSize(128 * systemPageSize))
                .get(1, SECONDS);

        PageMemoryImpl memory = memory(new long[]{100 * systemPageSize, 28 * systemPageSize});

        memory.start();

        try {
            long maxPages = memory.totalPages();

            long maxDirtyPages = (maxPages * 3 / 4);

            assertThat(maxDirtyPages, greaterThanOrEqualTo(50L));

            for (int i = 0; i < maxDirtyPages - 1; i++) {
                allocatePage(memory);

                assertTrue(memory.safeToUpdate(), "i=" + i);
            }

            for (int i = (int) maxDirtyPages - 1; i < maxPages; i++) {
                allocatePage(memory);

                assertFalse(memory.safeToUpdate(), "i=" + i);
            }

            // TODO: IGNITE-16984 After the checkpoint check assertTrue(memory.safeToUpdate())
        } finally {
            memory.stop(true);
        }
    }
}
