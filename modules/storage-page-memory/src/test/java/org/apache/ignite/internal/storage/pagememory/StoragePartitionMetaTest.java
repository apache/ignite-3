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

package org.apache.ignite.internal.storage.pagememory;

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.flag;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;
import static org.apache.ignite.internal.storage.pagememory.StoragePartitionMeta.partitionMetaPageId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.storage.pagememory.StoragePartitionMeta.StoragePartitionMetaSnapshot;
import org.junit.jupiter.api.Test;

/**
 * For {@link StoragePartitionMeta} testing.
 */
public class StoragePartitionMetaTest {
    @Test
    void testLastAppliedIndex() {
        StoragePartitionMeta meta = createMeta();

        assertEquals(0, meta.lastAppliedIndex());

        assertDoesNotThrow(() -> meta.lastApplied(null, 100, 10));

        assertEquals(100, meta.lastAppliedIndex());

        assertDoesNotThrow(() -> meta.lastApplied(UUID.randomUUID(), 500, 50));

        assertEquals(500, meta.lastAppliedIndex());
    }

    @Test
    void testLastAppliedTerm() {
        StoragePartitionMeta meta = createMeta();

        assertEquals(0, meta.lastAppliedTerm());

        meta.lastApplied(null, 100, 10);

        assertEquals(10, meta.lastAppliedTerm());

        meta.lastApplied(UUID.randomUUID(), 500, 50);

        assertEquals(50, meta.lastAppliedTerm());
    }

    @Test
    void testLastGroupConfig() {
        StoragePartitionMeta meta = createMeta();

        assertThat(meta.lastReplicationProtocolGroupConfigFirstPageId(), is(0L));

        meta.lastReplicationProtocolGroupConfigFirstPageId(null, 12);

        assertThat(meta.lastReplicationProtocolGroupConfigFirstPageId(), is(12L));

        meta.lastReplicationProtocolGroupConfigFirstPageId(UUID.randomUUID(), 34);

        assertThat(meta.lastReplicationProtocolGroupConfigFirstPageId(), is(34L));
    }

    @Test
    void testPageCount() {
        StoragePartitionMeta meta = createMeta();

        assertEquals(0, meta.pageCount());

        assertDoesNotThrow(() -> meta.incrementPageCount(null));

        assertEquals(1, meta.pageCount());

        assertDoesNotThrow(() -> meta.incrementPageCount(UUID.randomUUID()));

        assertEquals(2, meta.pageCount());
    }

    @Test
    void testVersionChainTreeRootPageId() {
        StoragePartitionMeta meta = createMeta();

        assertEquals(0, meta.versionChainTreeRootPageId());

        assertDoesNotThrow(() -> meta.versionChainTreeRootPageId(null, 100));

        assertEquals(100, meta.versionChainTreeRootPageId());

        assertDoesNotThrow(() -> meta.versionChainTreeRootPageId(UUID.randomUUID(), 500));

        assertEquals(500, meta.versionChainTreeRootPageId());
    }

    @Test
    void testFreeListRootPageId() {
        StoragePartitionMeta meta = createMeta();

        assertEquals(0, meta.freeListRootPageId());

        assertDoesNotThrow(() -> meta.freeListRootPageId(null, 100));

        assertEquals(100, meta.freeListRootPageId());

        assertDoesNotThrow(() -> meta.freeListRootPageId(UUID.randomUUID(), 500));

        assertEquals(500, meta.freeListRootPageId());
    }

    @Test
    void testEstimatedSize() {
        StoragePartitionMeta meta = createMeta();

        assertEquals(0, meta.estimatedSize());

        assertDoesNotThrow(() -> meta.incrementEstimatedSize(null));

        assertEquals(1, meta.estimatedSize());

        assertDoesNotThrow(() -> meta.incrementEstimatedSize(UUID.randomUUID()));

        assertEquals(2, meta.estimatedSize());

        assertDoesNotThrow(() -> meta.decrementEstimatedSize(null));

        assertEquals(1, meta.estimatedSize());

        assertDoesNotThrow(() -> meta.decrementEstimatedSize(UUID.randomUUID()));

        assertEquals(0, meta.estimatedSize());
    }

    @Test
    void testSnapshot() {
        StoragePartitionMeta meta = createMeta();

        UUID checkpointId = null;

        checkSnapshot(meta.metaSnapshot(checkpointId), 0, 0, 0, 0, 0, 0, 0, 0);
        checkSnapshot(meta.metaSnapshot(checkpointId = UUID.randomUUID()), 0, 0, 0, 0, 0, 0, 0, 0);

        meta.lastApplied(checkpointId, 50, 5);
        meta.lastReplicationProtocolGroupConfigFirstPageId(checkpointId, 12);
        meta.versionChainTreeRootPageId(checkpointId, 300);
        meta.freeListRootPageId(checkpointId, 900);
        meta.incrementPageCount(checkpointId);
        meta.incrementEstimatedSize(checkpointId);

        long wiHead = Long.MAX_VALUE;
        meta.updateWiHead(checkpointId, wiHead);

        checkSnapshot(meta.metaSnapshot(checkpointId), 0, 0, 0, 0, 0, 0, 0, 0L);
        checkSnapshot(meta.metaSnapshot(UUID.randomUUID()), 50, 5, 12, 300, 900, 1, 1, wiHead);

        meta.lastApplied(checkpointId = UUID.randomUUID(), 51, 6);
        meta.lastReplicationProtocolGroupConfigFirstPageId(checkpointId, 34);
        checkSnapshot(meta.metaSnapshot(checkpointId), 50, 5, 12, 300, 900, 1, 1, wiHead);

        meta.versionChainTreeRootPageId(checkpointId = UUID.randomUUID(), 303);
        checkSnapshot(meta.metaSnapshot(checkpointId), 51, 6, 34, 300, 900, 1, 1, wiHead);

        meta.freeListRootPageId(checkpointId = UUID.randomUUID(), 909);
        checkSnapshot(meta.metaSnapshot(checkpointId), 51, 6, 34, 303, 900, 1, 1, wiHead);

        meta.incrementPageCount(checkpointId = UUID.randomUUID());
        checkSnapshot(meta.metaSnapshot(checkpointId), 51, 6, 34, 303, 909, 1, 1, wiHead);

        checkSnapshot(meta.metaSnapshot(UUID.randomUUID()), 51, 6, 34, 303, 909, 2, 1, wiHead);

        meta.incrementEstimatedSize(checkpointId = UUID.randomUUID());
        checkSnapshot(meta.metaSnapshot(checkpointId), 51, 6, 34, 303, 909, 2, 1, wiHead);
        checkSnapshot(meta.metaSnapshot(UUID.randomUUID()), 51, 6, 34, 303, 909, 2, 2, wiHead);

        long newWiHead = Long.MAX_VALUE - 1;
        meta.updateWiHead(checkpointId = UUID.randomUUID(), newWiHead);
        checkSnapshot(meta.metaSnapshot(checkpointId), 51, 6, 34, 303, 909, 2, 2, wiHead);
        checkSnapshot(meta.metaSnapshot(UUID.randomUUID()), 51, 6, 34, 303, 909, 2, 2, newWiHead);
    }

    @Test
    void testStoragePartitionMetaPageId() {
        long pageId = partitionMetaPageId(666);

        assertEquals(666, partitionId(pageId));
        assertEquals(FLAG_AUX, flag(pageId));
        assertEquals(0, pageIndex(pageId));
    }

    @Test
    void testWIHead() {
        StoragePartitionMeta meta = createMeta();

        assertEquals(PageIdUtils.NULL_LINK, meta.wiHeadLink());

        meta.updateWiHead(null, 1234L);

        assertEquals(1234L, meta.wiHeadLink());

        meta.updateWiHead(UUID.randomUUID(), 5678L);

        assertEquals(5678L, meta.wiHeadLink());
    }

    private static void checkSnapshot(
            StoragePartitionMetaSnapshot snapshot,
            long expLastAppliedIndex,
            long expLastAppliedTerm,
            long expLastGroupConfigFirstPageId,
            long expVersionChainTreeRootPageId,
            long expFreeListRootPageId,
            int expPageCount,
            long expEstimatedSize,
            long expWiHead
    ) {
        assertThat(snapshot.lastAppliedIndex(), equalTo(expLastAppliedIndex));
        assertThat(snapshot.lastAppliedTerm(), equalTo(expLastAppliedTerm));
        assertThat(snapshot.lastReplicationProtocolGroupConfigFirstPageId(), equalTo(expLastGroupConfigFirstPageId));
        assertThat(snapshot.versionChainTreeRootPageId(), equalTo(expVersionChainTreeRootPageId));
        assertThat(snapshot.freeListRootPageId(), equalTo(expFreeListRootPageId));
        assertThat(snapshot.pageCount(), equalTo(expPageCount));
        assertThat(snapshot.estimatedSize(), equalTo(expEstimatedSize));
        assertThat(snapshot.wiHeadLink(), equalTo(expWiHead));
    }

    private static StoragePartitionMeta createMeta() {
        return new StoragePartitionMeta(0, 1, 0, 0, 0, 0, null, 0, 0, 0, 0, 0, 0, 0)
                .init(null);
    }
}
