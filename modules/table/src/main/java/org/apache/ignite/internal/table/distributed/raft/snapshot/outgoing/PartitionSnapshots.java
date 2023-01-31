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

package org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing;

import java.util.List;

/**
 * Provides read access to the list of ongoing snapshots of just one partition.
 * {@link #ongoingSnapshots()} must be accessed ONLY under the corresponding lock obtained via this interface.
 */
public interface PartitionSnapshots {
    /**
     * Acquires the read lock. This lock is required to access {@link #ongoingSnapshots()}.
     */
    void acquireReadLock();

    /**
     * Releases the read lock.
     */
    void releaseReadLock();

    /**
     * Returns snapshots that are currently active on the partition corresponding to this instance. This method must
     * only be called (and its result operated upon) under a lock acquired with {@link #acquireReadLock()}.
     *
     * @return List of snapshots currently active on the corresponding partition.
     */
    List<OutgoingSnapshot> ongoingSnapshots();
}
