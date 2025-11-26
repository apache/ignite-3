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

package org.apache.ignite.internal.raft.storage;

import org.jetbrains.annotations.Nullable;

/**
 * Used to calculate next group ID for storage (in log storage) from the previous one. Might be used to increase efficacy
 * of a scan over log storage key ranges containing different groupIDs: if we know we want to fast forward,
 * this instance will tell where to fast forward.
 */
@FunctionalInterface
public interface GroupIdFastForward {
    /**
     * Returns ID of the group (for storage) to which to fast forward from the given one. Returns {@code null} if no fast forward
     * is needed (in this case, the iteration will simply proceed with the next key).
     *
     * @param storageGroupId Current group ID from which we want to fast forward.
     */
    @Nullable String nextStorageGroupId(String storageGroupId);
}
