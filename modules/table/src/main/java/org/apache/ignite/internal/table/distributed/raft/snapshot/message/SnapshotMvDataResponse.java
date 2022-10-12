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

package org.apache.ignite.internal.table.distributed.raft.snapshot.message;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.distributed.TableMessageGroup;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.Marshallable;
import org.apache.ignite.network.annotations.Transferable;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot partition data response message.
 */
@Transferable(TableMessageGroup.SNAPSHOT_MV_DATA_RESPONSE)
public interface SnapshotMvDataResponse extends NetworkMessage {
    /** List of version chains. */
    List<ResponseEntry> rows();

    /** Flag that indicates whether this is the last response or not. */
    boolean finish();

    /**
     * Single row response as a message.
     */
    @SuppressWarnings("PublicInnerClass")
    @Transferable(TableMessageGroup.SNAPSHOT_MV_DATA_RESPONSE_ENTRY)
    interface ResponseEntry extends NetworkMessage {
        /** Individual row id. */
        UUID rowId();

        /** List of {@link BinaryRow}s for a given {@link #rowId()}. */
        List<ByteBuffer> rowVersions();

        /**
         * List of commit timestamps for all committed versions. Might be smaller than {@link #rowVersions()} if there's a write-intent
         * in the chain.
         */
        @Marshallable
        List<HybridTimestamp> timestamps();

        /** Transaction id for write-intent if it's present. */
        @Nullable UUID txId();

        /** Commit table id for write-intent if it's present. */
        @Nullable UUID commitTableId();

        /** Commit partition id for write-intent if it's present. {@code -1} otherwise. */
        int commitPartitionId();
    }
}
