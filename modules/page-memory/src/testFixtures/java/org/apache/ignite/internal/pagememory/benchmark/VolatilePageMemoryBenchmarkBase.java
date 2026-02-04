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

package org.apache.ignite.internal.pagememory.benchmark;

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;

import java.util.Random;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.configuration.VolatileDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.freelist.FreeList;
import org.apache.ignite.internal.pagememory.freelist.FreeListImpl;
import org.apache.ignite.internal.pagememory.inmemory.VolatilePageMemory;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.util.Constants;
import org.apache.ignite.internal.util.OffheapReadWriteLock;

/**
 * Base class for various benchmarks. Can start and stop a volatile data region. Also pre-allocates a free list, because all structures need
 * it anyway.
 */
public class VolatilePageMemoryBenchmarkBase {
    /** Size of a data region. Should be large enough to fit all the data. */
    private static final long REGION_SIZE = 4L * Constants.GiB;

    /** Page size. We may want to make it configurable in the future. Let's use a more performant 4Kb for now. */
    protected static final int PAGE_SIZE = 4 * Constants.KiB;

    /** Group ID constant for the benchmark. Could be anything. */
    protected static final int GROUP_ID = 1;
    /** Partition ID constant for the benchmark. Could be anything. */
    protected static final int PARTITION_ID = 0;

    /** An instance of {@link Random}. */
    protected static final Random RANDOM = new Random(System.currentTimeMillis());

    /** A {@link PageMemory} instance. */
    protected VolatilePageMemory volatilePageMemory;

    /** A {@link FreeListImpl} instance to be used as both {@link FreeList} and {@link ReuseList}. */
    protected FreeListImpl freeList;

    /**
     * Starts data region and pre-allocates a free list.
     */
    public void setup() throws Exception {
        var ioRegistry = new PageIoRegistry();
        ioRegistry.loadFromServiceLoader();

        volatilePageMemory = new VolatilePageMemory(
                VolatileDataRegionConfiguration.builder().pageSize(PAGE_SIZE).initSize(REGION_SIZE).maxSize(REGION_SIZE).build(),
                ioRegistry,
                new OffheapReadWriteLock(OffheapReadWriteLock.DEFAULT_CONCURRENCY_LEVEL)
        );

        freeList = new FreeListImpl(
                "freeList",
                GROUP_ID,
                PARTITION_ID,
                volatilePageMemory,
                volatilePageMemory.allocatePageNoReuse(GROUP_ID, PARTITION_ID, FLAG_AUX),
                true,
                null
        );
    }

    /**
     * Stops data region.
     */
    public void tearDown() throws Exception {
        volatilePageMemory.stop(true);
    }
}
