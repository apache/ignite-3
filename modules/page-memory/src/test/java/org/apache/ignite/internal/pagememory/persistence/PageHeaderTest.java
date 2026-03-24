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
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.dirty;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.fullPageId;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.initNew;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.isAcquired;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.partitionGeneration;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.pinCount;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.releasePage;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.tempBufferPointer;
import static org.apache.ignite.internal.pagememory.persistence.PageHeader.timestamp;
import static org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory.INVALID_REL_PTR;
import static org.apache.ignite.internal.util.GridUnsafe.allocateMemory;
import static org.apache.ignite.internal.util.GridUnsafe.freeMemory;
import static org.apache.ignite.internal.util.GridUnsafe.getLong;
import static org.apache.ignite.internal.util.GridUnsafe.zeroMemory;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.internal.pagememory.FullPageId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** For {@link PageHeader} testing. */
public class PageHeaderTest {
    private static long pageHeaderAddr = -1;

    @BeforeAll
    static void beforeAll() {
        pageHeaderAddr = allocateMemory(PAGE_OVERHEAD);
    }

    @AfterAll
    static void afterAll() {
        if (pageHeaderAddr != -1) {
            freeMemory(pageHeaderAddr);
        }
    }

    @BeforeEach
    void setUp() {
        initNew(pageHeaderAddr);
    }

    @AfterEach
    void tearDown() {
        zeroMemory(pageHeaderAddr, PAGE_OVERHEAD);
    }

    @Test
    void testInitNew() {
        zeroMemory(pageHeaderAddr, PAGE_OVERHEAD);
        initNew(pageHeaderAddr);

        assertEquals(0L, timestamp(pageHeaderAddr));
        assertEquals(UNKNOWN_PARTITION_GENERATION, partitionGeneration(pageHeaderAddr));
        assertFalse(dirty(pageHeaderAddr));
        assertEquals(new FullPageId(0, 0), fullPageId(pageHeaderAddr));
        assertEquals(0, pinCount(pageHeaderAddr));
        assertEquals(0L, getLong(pageHeaderAddr + PAGE_LOCK_OFFSET));
        assertEquals(INVALID_REL_PTR, tempBufferPointer(pageHeaderAddr));
    }

    @Test
    void testReadWriteTimeStamp() {
        long timestamp = System.currentTimeMillis();

        timestamp(pageHeaderAddr, timestamp);
        assertEquals(timestamp & ~0xFF, timestamp(pageHeaderAddr));
    }

    @Test
    void testReadWritePartitionGeneration() {
        partitionGeneration(pageHeaderAddr, 42);
        assertEquals(42, partitionGeneration(pageHeaderAddr));
    }

    @Test
    void testReadWriteDirtyFlagValue() {
        assertFalse(dirty(pageHeaderAddr, true));
        assertTrue(dirty(pageHeaderAddr));

        assertTrue(dirty(pageHeaderAddr, false));
        assertFalse(dirty(pageHeaderAddr));
    }

    @Test
    void testReadWriteFullPageId() {
        fullPageId(pageHeaderAddr, new FullPageId(4, 2));
        assertEquals(new FullPageId(4, 2), fullPageId(pageHeaderAddr));
    }

    @Test
    void testAcquireRelease() {
        assertEquals(0, acquirePage(pageHeaderAddr));
        assertEquals(1, pinCount(pageHeaderAddr));
        assertTrue(isAcquired(pageHeaderAddr));

        assertEquals(0, releasePage(pageHeaderAddr));
        assertEquals(0, pinCount(pageHeaderAddr));
        assertFalse(isAcquired(pageHeaderAddr));
    }

    @Test
    void testReadWriteCheckpointTempBufferRelativePointer() {
        tempBufferPointer(pageHeaderAddr, 42);
        assertEquals(42, tempBufferPointer(pageHeaderAddr));
    }
}
