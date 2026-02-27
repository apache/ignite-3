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

package org.apache.ignite.internal.raft.storage.segstore;

import org.jetbrains.annotations.Nullable;

/**
 * Provides information about a Raft group present in the storage.
 */
@FunctionalInterface
interface GroupInfoProvider {
    GroupInfoProvider NO_OP = groupId -> null;

    class GroupInfo {
        private final long firstLogIndexInclusive;

        private final long lastLogIndexExclusive;

        GroupInfo(long firstLogIndexInclusive, long lastLogIndexExclusive) {
            this.firstLogIndexInclusive = firstLogIndexInclusive;
            this.lastLogIndexExclusive = lastLogIndexExclusive;
        }

        long firstLogIndexInclusive() {
            return firstLogIndexInclusive;
        }

        long lastLogIndexExclusive() {
            return lastLogIndexExclusive;
        }
    }

    @Nullable
    GroupInfo groupInfo(long groupId);
}
