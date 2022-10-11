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

package org.apache.ignite.internal.table.distributed;

import org.apache.ignite.internal.table.distributed.message.HasDataRequest;
import org.apache.ignite.internal.table.distributed.message.HasDataResponse;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMetaRequest;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMetaResponse;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMvDataRequest;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMvDataResponse;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotMvDataResponse.ResponseEntry;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotTxDataRequest;
import org.apache.ignite.internal.table.distributed.raft.snapshot.message.SnapshotTxDataResponse;
import org.apache.ignite.internal.table.distributed.replication.request.ReadOnlyMultiRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadOnlyScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadOnlySingleRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteMultiRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteScanCloseReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteSingleRowReplicaRequest;
import org.apache.ignite.internal.table.distributed.replication.request.ReadWriteSwapRowReplicaRequest;
import org.apache.ignite.network.annotations.MessageGroup;

/**
 * Message group for the table module.
 */
@MessageGroup(groupType = 9, groupName = "TableMessages")
public interface TableMessageGroup {
    /**
     * Message type for {@link ReadWriteSingleRowReplicaRequest}.
     */
    short RW_SINGLE_ROW_REPLICA_REQUEST = 0;

    /**
     * Message type for {@link ReadWriteMultiRowReplicaRequest}.
     */
    short RW_MULTI_ROW_REPLICA_REQUEST = 1;

    /**
     * Message type for {@link ReadWriteSwapRowReplicaRequest}.
     */
    short RW_DUAL_ROW_REPLICA_REQUEST = 2;

    /**
     * Message type for {@link ReadWriteScanRetrieveBatchReplicaRequest}.
     */
    short RW_SCAN_RETRIEVE_BATCH_REPLICA_REQUEST = 3;

    /**
     * Message type for {@link ReadWriteScanCloseReplicaRequest}.
     */
    short RW_SCAN_CLOSE_REPLICA_REQUEST = 4;

    /**
     * Message type for {@link HasDataRequest}.
     */
    short HAS_DATA_REQUEST = 5;

    /**
     * Message type for {@link HasDataResponse}.
     */
    short HAS_DATA_RESPONSE = 6;

    /**
     * Message type for {@link ReadOnlySingleRowReplicaRequest}.
     */
    short RO_SINGLE_ROW_REPLICA_REQUEST = 7;

    /**
     * Message type for {@link ReadOnlyMultiRowReplicaRequest}.
     */
    short RO_MULTI_ROW_REPLICA_REQUEST = 8;

    /**
     * Message type for {@link ReadOnlyScanRetrieveBatchReplicaRequest}.
     */
    short RO_SCAN_RETRIEVE_BATCH_REPLICA_REQUEST = 9;

    /**
     * Message type for {@link SnapshotMetaRequest}.
     */
    short SNAPSHOT_META_REQUEST = 10;

    /**
     * Message type for {@link SnapshotMetaResponse}.
     */
    short SNAPSHOT_META_RESPONSE = 11;

    /**
     * Message type for {@link SnapshotMvDataRequest}.
     */
    short SNAPSHOT_MV_DATA_REQUEST = 12;

    /**
     * Message type for {@link ResponseEntry}.
     */
    short SNAPSHOT_MV_DATA_RESPONSE_ENTRY = 13;

    /**
     * Message type for {@link SnapshotMvDataResponse}.
     */
    short SNAPSHOT_MV_DATA_RESPONSE = 14;

    /**
     * Message type for {@link SnapshotTxDataRequest}.
     */
    short SNAPSHOT_TX_DATA_REQUEST = 15;

    /**
     * Message type for {@link SnapshotTxDataResponse}.
     */
    short SNAPSHOT_TX_DATA_RESPONSE = 16;
}
