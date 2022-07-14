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

import java.util.Collection;
import org.apache.ignite.internal.pagememory.FullPageId;

/**
 * Dirty pages collection container.
 */
public class DirtyPagesCollection {
    private final Collection<FullPageId> pageIds;

    private final Collection<GroupPartitionId> partitionIds;

    /**
     * Constructor.
     *
     * <p>Does not copy collections.
     *
     * @param pageIds Dirty page IDs.
     * @param partitionIds Dirty partitions IDs.
     */
    public DirtyPagesCollection(Collection<FullPageId> pageIds, Collection<GroupPartitionId> partitionIds) {
        this.pageIds = pageIds;
        this.partitionIds = partitionIds;
    }

    /**
     * Returns dirty page IDs.
     */
    public Collection<FullPageId> pageIds() {
        return pageIds;
    }

    /**
     * Returns dirty partition IDs.
     */
    public Collection<GroupPartitionId> partitionIds() {
        return partitionIds;
    }
}
