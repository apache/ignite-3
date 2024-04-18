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
// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: raft.proto

package org.apache.ignite.raft.jraft.entity;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.network.annotations.Marshallable;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.raft.jraft.RaftMessageGroup;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.jetbrains.annotations.Nullable;

public final class RaftOutter {
    @Transferable(RaftMessageGroup.RaftOutterMessageGroup.ENTRY_META)
    public interface EntryMeta extends Message {
        long term();

        @Marshallable
        EnumOutter.EntryType type();

        @Nullable Collection<String> peersList();

        long dataLen();

        @Nullable Collection<String> oldPeersList();

        long checksum();

        @Nullable Collection<String> learnersList();

        @Nullable Collection<String> oldLearnersList();

        /** Returns {@code true} when the entry has a checksum, {@code false} otherwise. */
        boolean hasChecksum();
    }

    @Transferable(RaftMessageGroup.RaftOutterMessageGroup.SNAPSHOT_META)
    public interface SnapshotMeta extends Message {
        long lastIncludedIndex();

        long lastIncludedTerm();

        @Nullable Collection<String> peersList();

        @Nullable Collection<String> oldPeersList();

        @Nullable Collection<String> learnersList();

        @Nullable Collection<String> oldLearnersList();

        /** Minimum catalog version that is required for the snapshot to be accepted by a follower. */
        int requiredCatalogVersion();

        /** Returns the row ID for which the index needs to be built per building index ID at the time the snapshot meta was created. */
        @Nullable Map<Integer, UUID> nextRowIdToBuildByIndexId();
    }
}
