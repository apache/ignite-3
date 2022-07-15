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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.jetbrains.annotations.Nullable;

/**
 * Partition meta information manager.
 */
// TODO: IGNITE-17132 Do not forget about deleting the partition meta information
public class PartitionMetaManager {
    private final Map<GroupPartitionId, PartitionMeta> metas = new ConcurrentHashMap<>();

    /**
     * Returns the partition's meta information.
     *
     * @param groupPartitionId Partition of the group.
     */
    public @Nullable PartitionMeta getMeta(GroupPartitionId groupPartitionId) {
        return metas.get(groupPartitionId);
    }

    /**
     * Adds partition meta information.
     *
     * @param groupPartitionId Partition of the group.
     * @param partitionMeta Partition meta information.
     */
    public void addMeta(GroupPartitionId groupPartitionId, PartitionMeta partitionMeta) {
        metas.put(groupPartitionId, partitionMeta);
    }
}
