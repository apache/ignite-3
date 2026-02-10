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

import static org.apache.ignite.internal.pagememory.persistence.PageHeader.UNKNOWN_PARTITION_GENERATION;

import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.pagememory.FullPageId;

/** Extension for checkpoint with partition generation at the time the page becomes dirty. */
public final class DirtyFullPageId extends FullPageId {
    /** Null dirty full page ID. */
    public static final DirtyFullPageId NULL_PAGE = new DirtyFullPageId(-1, -1, UNKNOWN_PARTITION_GENERATION);

    private final int partitionGeneration;

    /**
     * Constructor.
     *
     * @param pageId Page ID.
     * @param groupId Group ID.
     * @param partitionGeneration Partition generation.
     */
    public DirtyFullPageId(long pageId, int groupId, int partitionGeneration) {
        super(pageId, groupId);

        this.partitionGeneration = partitionGeneration;
    }

    /**
     * Constructor.
     *
     * @param fullPageId Full page ID.
     * @param partitionGeneration Partition generation.
     */
    public DirtyFullPageId(FullPageId fullPageId, int partitionGeneration) {
        this(fullPageId.pageId(), fullPageId.groupId(), partitionGeneration);
    }

    /** Returns partition generation. */
    public int partitionGeneration() {
        return partitionGeneration;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DirtyFullPageId that = (DirtyFullPageId) o;

        return effectivePageId() == that.effectivePageId()
                && groupId() == that.groupId()
                && partitionGeneration == that.partitionGeneration;
    }

    @Override
    public int hashCode() {
        return (int) (mix64(effectivePageId()) ^ mix32(groupId()) ^ mix32(partitionGeneration));
    }

    @Override
    public String toString() {
        return new IgniteStringBuilder("DirtyFullPageId [pageId=").appendHex(pageId())
                .app(", effectivePageId=").appendHex(effectivePageId())
                .app(", groupId=").app(groupId())
                .app(", partitionGeneration=").app(partitionGeneration)
                .app(']').toString();
    }

    /** Converts to {@link FullPageId}. */
    public FullPageId toFullPageId() {
        return new FullPageId(pageId(), groupId());
    }
}
