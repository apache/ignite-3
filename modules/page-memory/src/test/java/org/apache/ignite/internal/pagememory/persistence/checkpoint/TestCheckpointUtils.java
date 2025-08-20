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

package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointDirtyPages.DIRTY_PAGE_COMPARATOR;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;

import java.util.Arrays;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.persistence.DirtyFullPageId;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;

/** Helper class for checkpoint testing that may contain useful methods and constants. */
// TODO: IGNITE-26233 Может тут все поправить нужно будет
class TestCheckpointUtils {
    /** Sorts dirty pages and creates a new instance {@link DirtyPagesAndPartitions}. */
    static DirtyPagesAndPartitions createDirtyPagesAndPartitions(PersistentPageMemory pageMemory, FullPageId... dirtyPages) {
        DirtyFullPageId[] dirtyFullPageIds = Arrays.stream(dirtyPages)
                .map(fullPageId -> new DirtyFullPageId(fullPageId.pageId(), fullPageId.groupId(), fullPageId.groupId()))
                .toArray(DirtyFullPageId[]::new);

        Arrays.sort(dirtyFullPageIds, DIRTY_PAGE_COMPARATOR);

        return new DirtyPagesAndPartitions(
                pageMemory,
                dirtyFullPageIds,
                Arrays.stream(dirtyPages).map(GroupPartitionId::convert).collect(toSet())
        );
    }

    /**
     * Creates new full page ID.
     *
     * @param groupId Group ID.
     * @param partitionId Partition ID.
     */
    static FullPageId fullPageId(int groupId, int partitionId) {
        return new FullPageId(pageId(partitionId, FLAG_DATA, 0), groupId);
    }
}
