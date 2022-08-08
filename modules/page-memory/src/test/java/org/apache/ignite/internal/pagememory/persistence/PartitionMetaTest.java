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

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;
import static org.apache.ignite.internal.pagememory.persistence.PartitionMeta.partitionMetaPageId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.flag;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta.PartitionMetaSnapshot;
import org.junit.jupiter.api.Test;

/**
 * For {@link PartitionMeta} testing.
 */
public class PartitionMetaTest {
    @Test
    void testTreeRootPageId() {
        PartitionMeta meta = new PartitionMeta();

        assertEquals(0, meta.treeRootPageId());

        assertDoesNotThrow(() -> meta.treeRootPageId(null, 100));

        assertEquals(100, meta.treeRootPageId());

        assertDoesNotThrow(() -> meta.treeRootPageId(UUID.randomUUID(), 500));

        assertEquals(500, meta.treeRootPageId());
    }

    @Test
    void testReuseListRootPageId() {
        PartitionMeta meta = new PartitionMeta();

        assertEquals(0, meta.reuseListRootPageId());

        assertDoesNotThrow(() -> meta.reuseListRootPageId(null, 100));

        assertEquals(100, meta.reuseListRootPageId());

        assertDoesNotThrow(() -> meta.reuseListRootPageId(UUID.randomUUID(), 500));

        assertEquals(500, meta.reuseListRootPageId());
    }

    @Test
    void testPageCount() {
        PartitionMeta meta = new PartitionMeta();

        assertEquals(0, meta.pageCount());

        assertDoesNotThrow(() -> meta.incrementPageCount(null));

        assertEquals(1, meta.pageCount());

        assertDoesNotThrow(() -> meta.incrementPageCount(UUID.randomUUID()));

        assertEquals(2, meta.pageCount());
    }

    @Test
    void testVersionChainTreeRootPageId() {
        PartitionMeta meta = new PartitionMeta();

        assertEquals(0, meta.versionChainTreeRootPageId());

        assertDoesNotThrow(() -> meta.versionChainTreeRootPageId(null, 100));

        assertEquals(100, meta.versionChainTreeRootPageId());

        assertDoesNotThrow(() -> meta.versionChainTreeRootPageId(UUID.randomUUID(), 500));

        assertEquals(500, meta.versionChainTreeRootPageId());
    }

    @Test
    void testVersionChainFreeListRootPageId() {
        PartitionMeta meta = new PartitionMeta();

        assertEquals(0, meta.versionChainFreeListRootPageId());

        assertDoesNotThrow(() -> meta.versionChainFreeListRootPageId(null, 100));

        assertEquals(100, meta.versionChainFreeListRootPageId());

        assertDoesNotThrow(() -> meta.versionChainFreeListRootPageId(UUID.randomUUID(), 500));

        assertEquals(500, meta.versionChainFreeListRootPageId());
    }

    @Test
    void testRowVersionFreeListRootPageId() {
        PartitionMeta meta = new PartitionMeta();

        assertEquals(0, meta.rowVersionFreeListRootPageId());

        assertDoesNotThrow(() -> meta.rowVersionFreeListRootPageId(null, 100));

        assertEquals(100, meta.rowVersionFreeListRootPageId());

        assertDoesNotThrow(() -> meta.rowVersionFreeListRootPageId(UUID.randomUUID(), 500));

        assertEquals(500, meta.rowVersionFreeListRootPageId());
    }

    @Test
    void testSnapshot() {
        UUID checkpointId = null;

        PartitionMeta meta = new PartitionMeta(checkpointId, 0, 0, 0, 0, 0, 0);

        checkSnapshot(meta.metaSnapshot(checkpointId), 0, 0, 0, 0, 0, 0);
        checkSnapshot(meta.metaSnapshot(checkpointId = UUID.randomUUID()), 0, 0, 0, 0, 0, 0);

        meta.treeRootPageId(checkpointId, 100);
        meta.reuseListRootPageId(checkpointId, 500);
        meta.versionChainTreeRootPageId(checkpointId, 300);
        meta.versionChainFreeListRootPageId(checkpointId, 600);
        meta.rowVersionFreeListRootPageId(checkpointId, 900);
        meta.incrementPageCount(checkpointId);

        checkSnapshot(meta.metaSnapshot(checkpointId), 0, 0, 0, 0, 0, 0);
        checkSnapshot(meta.metaSnapshot(UUID.randomUUID()), 100, 500, 300, 600, 900, 1);

        meta.treeRootPageId(checkpointId = UUID.randomUUID(), 101);
        checkSnapshot(meta.metaSnapshot(checkpointId), 100, 500, 300, 600, 900, 1);

        meta.reuseListRootPageId(checkpointId = UUID.randomUUID(), 505);
        checkSnapshot(meta.metaSnapshot(checkpointId), 101, 500, 300, 600, 900, 1);

        meta.versionChainTreeRootPageId(checkpointId = UUID.randomUUID(), 303);
        checkSnapshot(meta.metaSnapshot(checkpointId), 101, 505, 300, 600, 900, 1);

        meta.versionChainFreeListRootPageId(checkpointId = UUID.randomUUID(), 606);
        checkSnapshot(meta.metaSnapshot(checkpointId), 101, 505, 303, 600, 900, 1);

        meta.rowVersionFreeListRootPageId(checkpointId = UUID.randomUUID(), 909);
        checkSnapshot(meta.metaSnapshot(checkpointId), 101, 505, 303, 606, 900, 1);

        meta.incrementPageCount(checkpointId = UUID.randomUUID());
        checkSnapshot(meta.metaSnapshot(checkpointId), 101, 505, 303, 606, 909, 1);

        checkSnapshot(meta.metaSnapshot(UUID.randomUUID()), 101, 505, 303, 606, 909, 2);
    }

    @Test
    void testPartitionMetaPageId() {
        long pageId = partitionMetaPageId(666);

        assertEquals(666, partitionId(pageId));
        assertEquals(FLAG_AUX, flag(pageId));
        assertEquals(0, pageIndex(pageId));
    }

    private static void checkSnapshot(
            PartitionMetaSnapshot snapshot,
            long expTreeRootPageId,
            long expReuseListPageId,
            long expVersionChainTreeRootPageId,
            long expVersionChainFreeListRootPageId,
            long expRowVersionFreeListRootPageId,
            int expPageCount
    ) {
        assertThat(snapshot.treeRootPageId(), equalTo(expTreeRootPageId));
        assertThat(snapshot.reuseListRootPageId(), equalTo(expReuseListPageId));
        assertThat(snapshot.versionChainTreeRootPageId(), equalTo(expVersionChainTreeRootPageId));
        assertThat(snapshot.versionChainFreeListRootPageId(), equalTo(expVersionChainFreeListRootPageId));
        assertThat(snapshot.rowVersionFreeListRootPageId(), equalTo(expRowVersionFreeListRootPageId));
        assertThat(snapshot.pageCount(), equalTo(expPageCount));
    }
}
