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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Partition Destruction Lock Manager. */
// TODO: IGNITE-26339 Make partition generation increase non-blocking
public class PartitionDestructionLockManager {
    private final Map<GroupPartitionId, ReentrantReadWriteLock> lockByPartitionId = new ConcurrentHashMap<>();

    /**
     * Returns a lock to synchronize between partition destruction and write operations to it.
     *
     * <p>For partition write operations you need to use {@link ReadWriteLock#readLock()} and for partition destruction
     * {@link ReadWriteLock#writeLock()}.</p>
     */
    public ReadWriteLock destructionLock(GroupPartitionId groupPartitionId) {
        return lockByPartitionId.computeIfAbsent(groupPartitionId, unused -> new ReentrantReadWriteLock());
    }

    /** Removes all locks for a group. */
    public void removeLockForGroup(int groupId) {
        lockByPartitionId.entrySet().removeIf(e -> e.getKey().getGroupId() == groupId);
    }
}
