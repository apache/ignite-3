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
import static org.apache.ignite.internal.util.Constants.MiB;

import java.util.stream.LongStream;
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
    protected PageMemory memory() throws Exception {
        dataRegionCfg
                .change(cfg -> cfg.changePageSize(PAGE_SIZE).changeInitSize(MAX_MEMORY_SIZE).changeMaxSize(MAX_MEMORY_SIZE))
                .get(1, SECONDS);

        PageIoRegistry ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        return new PageMemoryImpl(
                new UnsafeMemoryProvider(null),
                dataRegionCfg,
                ioRegistry,
                LongStream.range(0, 10).map(i -> 5 * MiB).toArray(),
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
}
