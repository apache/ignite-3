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
        PartitionMeta meta = new PartitionMeta(null, 0, 0, 0);

        assertEquals(0, meta.treeRootPageId());

        assertDoesNotThrow(() -> meta.treeRootPageId(null, 100));

        assertEquals(100, meta.treeRootPageId());

        assertDoesNotThrow(() -> meta.treeRootPageId(UUID.randomUUID(), 500));

        assertEquals(500, meta.treeRootPageId());
    }

    @Test
    void testReuseListRootPageId() {
        PartitionMeta meta = new PartitionMeta(null, 0, 0, 0);

        assertEquals(0, meta.reuseListRootPageId());

        assertDoesNotThrow(() -> meta.reuseListRootPageId(null, 100));

        assertEquals(100, meta.reuseListRootPageId());

        assertDoesNotThrow(() -> meta.reuseListRootPageId(UUID.randomUUID(), 500));

        assertEquals(500, meta.reuseListRootPageId());
    }

    @Test
    void testPageCount() {
        PartitionMeta meta = new PartitionMeta(null, 0, 0, 0);

        assertEquals(0, meta.pageCount());

        assertDoesNotThrow(() -> meta.incrementPageCount(null));

        assertEquals(1, meta.pageCount());

        assertDoesNotThrow(() -> meta.incrementPageCount(UUID.randomUUID()));

        assertEquals(2, meta.pageCount());
    }

    @Test
    void testSnapshot() {
        UUID checkpointId = null;

        PartitionMeta meta = new PartitionMeta(checkpointId, 0, 0, 0);

        checkSnapshot(meta.metaSnapshot(checkpointId), 0, 0, 0);
        checkSnapshot(meta.metaSnapshot(checkpointId = UUID.randomUUID()), 0, 0, 0);

        meta.treeRootPageId(checkpointId, 100);
        meta.reuseListRootPageId(checkpointId, 500);
        meta.incrementPageCount(checkpointId);

        checkSnapshot(meta.metaSnapshot(checkpointId), 0, 0, 0);
        checkSnapshot(meta.metaSnapshot(UUID.randomUUID()), 100, 500, 1);

        meta.treeRootPageId(checkpointId = UUID.randomUUID(), 101);
        checkSnapshot(meta.metaSnapshot(checkpointId), 100, 500, 1);

        meta.reuseListRootPageId(checkpointId = UUID.randomUUID(), 505);
        checkSnapshot(meta.metaSnapshot(checkpointId), 101, 500, 1);

        meta.incrementPageCount(checkpointId = UUID.randomUUID());
        checkSnapshot(meta.metaSnapshot(checkpointId), 101, 505, 1);

        checkSnapshot(meta.metaSnapshot(UUID.randomUUID()), 101, 505, 2);
    }

    private static void checkSnapshot(PartitionMetaSnapshot snapshot, long expTreeRootPageId, long reuseListPageId, int pageCount) {
        assertThat(snapshot.treeRootPageId(), equalTo(expTreeRootPageId));
        assertThat(snapshot.reuseListRootPageId(), equalTo(reuseListPageId));
        assertThat(snapshot.pageCount(), equalTo(pageCount));
    }
}
