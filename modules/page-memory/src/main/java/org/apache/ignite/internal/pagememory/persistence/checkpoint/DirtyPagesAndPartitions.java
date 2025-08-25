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

import java.util.Set;
import org.apache.ignite.internal.pagememory.persistence.DirtyFullPageId;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;

/** Container of dirty pages and partitions. */
class DirtyPagesAndPartitions {
    final PersistentPageMemory pageMemory;

    final DirtyFullPageId[] dirtyPages;

    final Set<GroupPartitionId> dirtyPartitions;

    DirtyPagesAndPartitions(PersistentPageMemory pageMemory, DirtyFullPageId[] dirtyPages, Set<GroupPartitionId> dirtyPartitions) {
        this.pageMemory = pageMemory;
        this.dirtyPages = dirtyPages;
        this.dirtyPartitions = dirtyPartitions;
    }
}
