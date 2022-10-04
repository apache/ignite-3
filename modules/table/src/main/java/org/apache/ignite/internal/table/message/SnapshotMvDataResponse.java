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

package org.apache.ignite.internal.table.message;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.Marshallable;
import org.apache.ignite.network.annotations.Transferable;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot partition data response message.
 */
@Transferable(TableMessageGroup.SNAPSHOT_MV_DATA_RESPONSE)
public interface SnapshotMvDataResponse extends NetworkMessage {
    Collection<SnapshotMvDataSingleResponse> rows();

    boolean finish();

    /**
     * Single row response as a message.
     */
    @Transferable(TableMessageGroup.SNAPSHOT_MV_DATA_SINGLE_RESPONSE)
    interface SnapshotMvDataSingleResponse extends NetworkMessage {
        @Marshallable
        List<ByteBuffer> rowVersions();

        @Marshallable
        List<HybridTimestamp> timestamps();

        UUID rowId();

        @Nullable UUID txId();

        @Nullable UUID commitTableId();

        int commitPartitionId();
    }
}
