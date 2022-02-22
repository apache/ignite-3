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
import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;

import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.tostring.S;

/**
 * Pair of group ID with partition ID. Immutable, comparable class, may be used as key in maps.
 */
public class GroupPartitionId implements Comparable<GroupPartitionId> {
    /** Index for super(meta) page. There is always such page for iterated cache partition. */
    private static final int METAPAGE_IDX = 0;

    /** Group ID. */
    private final int grpId;

    /** Partition ID. */
    private final int partId;

    /**
     * Creates group-partition tuple.
     *
     * @param grpId Group ID.
     * @param partId Partition ID.
     */
    public GroupPartitionId(final int grpId, final int partId) {
        this.grpId = grpId;
        this.partId = partId;
    }

    /**
     * Returns flag to be used for partition.
     *
     * @param partId Partition ID.
     */
    public static byte getFlagByPartId(final int partId) {
        return partId == INDEX_PARTITION ? FLAG_AUX : FLAG_DATA;
    }

    /**
     * Return group ID.
     */
    public int getGroupId() {
        return grpId;
    }

    /**
     * Return partition ID.
     */
    public int getPartitionId() {
        return partId;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(GroupPartitionId.class, this);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GroupPartitionId key = (GroupPartitionId) o;

        if (grpId != key.grpId) {
            return false;
        }

        return partId == key.partId;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        int result = grpId;

        result = 31 * result + partId;

        return result;
    }

    /** {@inheritDoc} */
    @Override
    public int compareTo(GroupPartitionId o) {
        if (getGroupId() < o.getGroupId()) {
            return -1;
        }

        if (getGroupId() > o.getGroupId()) {
            return 1;
        }

        if (getPartitionId() < o.getPartitionId()) {
            return -1;
        }

        if (getPartitionId() > o.getPartitionId()) {
            return 1;
        }

        return 0;
    }

    /**
     * Returns page ID (64 bits) constructed from partition ID and given index.
     *
     * @param pageIdx Page Index, monotonically growing number within each partition.
     */
    private long createPageId(final int pageIdx) {
        final int partId = getPartitionId();

        return pageId(partId, getFlagByPartId(partId), pageIdx);
    }

    /**
     * Returns Full page ID. For index 0 will return super-page of next partition
     *
     * @param pageIdx Page Index, monotonically growing number within each partition
     * @return FullPageId consists of cache ID (32 bits) and page ID (64 bits).
     */
    private FullPageId createFullPageId(final int pageIdx) {
        return new FullPageId(createPageId(pageIdx), getGroupId());
    }

    /**
     * Returns will return super-page (metapage) of this partition.
     */
    public FullPageId createFirstPageFullId() {
        return createFullPageId(METAPAGE_IDX);
    }
}
