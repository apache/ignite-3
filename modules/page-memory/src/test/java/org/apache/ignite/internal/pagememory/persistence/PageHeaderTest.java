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

package org.apache.ignite.internal.pagememory.persistence;

import static org.apache.ignite.internal.pagememory.persistence.PageHeader.PAGE_LOCK_OFFSET;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.PAGE_OVERHEAD;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.UNKNOWN_PARTITION_GENERATION;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.acquirePage;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.initNew;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.isAcquired;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.readAcquiresCount;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.readCheckpointTempBufferRelativePointer;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.readDirtyFlag;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.readFullPageId;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.readPartitionGeneration;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.readTimestamp;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.releasePage;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.writeCheckpointTempBufferRelativePointer;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.writeDirtyFlag;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.writeFullPageId;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.writePartitionGeneration;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.writeTimestamp;
import static org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory.INVALID_REL_PTR;
import static org.apache.ignite.internal.util.GridUnsafe.allocateMemory;
import static org.apache.ignite.internal.util.GridUnsafe.freeMemory;
import static org.apache.ignite.internal.util.GridUnsafe.getLong;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.internal.pagememory.FullPageId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** For {@link PageHeader} testing. */
public class PageHeaderTest {
    private long pageHeaderAddr = -1;

    @BeforeEach
    void setUp() {
        pageHeaderAddr = allocateMemory(PAGE_OVERHEAD);
    }

    @AfterEach
    void tearDown() {
        if (pageHeaderAddr == -1) {
            freeMemory(pageHeaderAddr);
        }
    }

    @Test
    void testInitNew() {
        initNew(pageHeaderAddr);

        assertEquals(0L, readTimestamp(pageHeaderAddr));
        assertEquals(UNKNOWN_PARTITION_GENERATION, readPartitionGeneration(pageHeaderAddr));
        assertFalse(readDirtyFlag(pageHeaderAddr));
        assertEquals(new FullPageId(0, 0), readFullPageId(pageHeaderAddr));
        assertEquals(0, readAcquiresCount(pageHeaderAddr));
        assertEquals(0L, getLong(pageHeaderAddr + PAGE_LOCK_OFFSET));
        assertEquals(INVALID_REL_PTR, readCheckpointTempBufferRelativePointer(pageHeaderAddr));
    }

    @Test
    void testReadWriteTimeStamp() {
        initNew(pageHeaderAddr);

        long timestamp = System.currentTimeMillis();

        writeTimestamp(pageHeaderAddr, timestamp);
        assertEquals(timestamp & ~0xFF, readTimestamp(pageHeaderAddr));
    }

    @Test
    void testReadWritePartitionGeneration() {
        initNew(pageHeaderAddr);

        writePartitionGeneration(pageHeaderAddr, 42);
        assertEquals(42, readPartitionGeneration(pageHeaderAddr));
    }

    @Test
    void testReadWriteDirtyFlagValue() {
        initNew(pageHeaderAddr);

        writeDirtyFlag(pageHeaderAddr, true);
        assertTrue(readDirtyFlag(pageHeaderAddr));

        writeDirtyFlag(pageHeaderAddr, false);
        assertFalse(readDirtyFlag(pageHeaderAddr));
    }

    @Test
    void testReadWriteFullPageId() {
        initNew(pageHeaderAddr);

        writeFullPageId(pageHeaderAddr, new FullPageId(4, 2));
        assertEquals(new FullPageId(4, 2), readFullPageId(pageHeaderAddr));
    }

    @Test
    void testAcquireRelease() {
        initNew(pageHeaderAddr);

        assertEquals(1, acquirePage(pageHeaderAddr));
        assertEquals(1, readAcquiresCount(pageHeaderAddr));
        assertTrue(isAcquired(pageHeaderAddr));

        assertEquals(0, releasePage(pageHeaderAddr));
        assertEquals(0, readAcquiresCount(pageHeaderAddr));
        assertFalse(isAcquired(pageHeaderAddr));
    }

    @Test
    void testReadWriteCheckpointTempBufferRelativePointer() {
        initNew(pageHeaderAddr);

        writeCheckpointTempBufferRelativePointer(pageHeaderAddr, 42);
        assertEquals(42, readCheckpointTempBufferRelativePointer(pageHeaderAddr));
    }
}
