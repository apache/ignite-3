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

package org.apache.ignite.internal.partition.replicator.network;

import static org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup.GROUP_TYPE;

import org.apache.ignite.internal.network.annotations.MessageGroup;
import org.apache.ignite.internal.partition.replicator.network.command.BuildIndexCommand;
import org.apache.ignite.internal.partition.replicator.network.command.FinishTxCommand;
import org.apache.ignite.internal.partition.replicator.network.command.TimedBinaryRowMessage;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateAllCommand;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateCommand;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateMinimumActiveTxStartTimeCommand;
import org.apache.ignite.internal.partition.replicator.network.command.WriteIntentSwitchCommand;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateMessage;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStatesRequest;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStatesResponse;
import org.apache.ignite.internal.partition.replicator.network.message.HasDataRequest;
import org.apache.ignite.internal.partition.replicator.network.message.HasDataResponse;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMetaRequest;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMetaResponse;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMvDataRequest;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMvDataResponse;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotMvDataResponse.ResponseEntry;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotTxDataRequest;
import org.apache.ignite.internal.partition.replicator.network.raft.SnapshotTxDataResponse;
import org.apache.ignite.internal.partition.replicator.network.replication.BinaryRowMessage;
import org.apache.ignite.internal.partition.replicator.network.replication.BinaryTupleMessage;
import org.apache.ignite.internal.partition.replicator.network.replication.BuildIndexReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.GetEstimatedSizeRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyDirectMultiRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyDirectSingleRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyMultiRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlySingleRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteMultiRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteMultiRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteScanRetrieveBatchReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteSingleRowPkReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteSingleRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ReadWriteSwapRowReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.ScanCloseReplicaRequest;
import org.apache.ignite.internal.partition.replicator.network.replication.UpdateMinimumActiveTxStartTimeReplicaRequest;

/**
 * Message group for the table module.
 */
@MessageGroup(groupType = GROUP_TYPE, groupName = "PartitionReplicationMessages")
public interface PartitionReplicationMessageGroup {
    /** Zone message group type. */
    short GROUP_TYPE = 9;

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
     * Message type for {@link ScanCloseReplicaRequest}.
     */
    short SCAN_CLOSE_REPLICA_REQUEST = 4;

    /**
     * Message type for {@link HasDataRequest}.
     */
    short HAS_DATA_REQUEST = 5;

    /**
     * Message type for {@link HasDataResponse}.
     */
    short HAS_DATA_RESPONSE = 6;

    /**
     * Message type for {@link ReadOnlySingleRowPkReplicaRequest}.
     */
    short RO_SINGLE_ROW_REPLICA_REQUEST = 7;

    /**
     * Message type for {@link ReadOnlyMultiRowPkReplicaRequest}.
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

    /**
     * Message type for {@link BinaryTupleMessage}.
     */
    short BINARY_TUPLE = 17;

    /**
     * Message type for {@link BinaryRowMessage}.
     */
    short BINARY_ROW_MESSAGE = 18;

    /**
     * Message type for {@link ReadWriteSingleRowPkReplicaRequest}.
     */
    short RW_SINGLE_ROW_PK_REPLICA_REQUEST = 19;

    /**
     * Message type for {@link ReadWriteMultiRowPkReplicaRequest}.
     */
    short RW_MULTI_ROW_PK_REPLICA_REQUEST = 20;

    /** Message type for {@link BuildIndexReplicaRequest}. */
    short BUILD_INDEX_REPLICA_REQUEST = 21;

    /**
     * Message type for {@link ReadOnlyDirectSingleRowReplicaRequest}.
     */
    short RO_DIRECT_SINGLE_ROW_REPLICA_REQUEST = 22;

    /**
     * Message type for {@link ReadOnlyDirectMultiRowReplicaRequest}.
     */
    short RO_DIRECT_MULTI_ROW_REPLICA_REQUEST = 23;

    /**
     * Message type for {@link TimedBinaryRowMessage}.
     */
    short TIMED_BINARY_ROW_MESSAGE = 24;

    /**
     * Message type for {@link GetEstimatedSizeRequest}.
     */
    short GET_ESTIMATED_SIZE_MESSAGE = 25;

    /**
     * Message type for {@link UpdateMinimumActiveTxStartTimeReplicaRequest}.
     */
    short UPDATE_MINIMUM_ACTIVE_TX_TIME_REPLICA_REQUEST = 26;

    /**
     * Message types for partition replicator module RAFT commands.
     *
     * <p>NOTE: Commands must be immutable because they will be stored in the replication log.</p>
     */
    interface Commands {
        /** Message type for {@link FinishTxCommand}. */
        short FINISH_TX = 40;

        /** Message type for {@link WriteIntentSwitchCommand}. */
        short WRITE_INTENT_SWITCH = 41;

        /** Message type for {@link UpdateAllCommand}. */
        short UPDATE_ALL = 42;

        /** Message type for {@link UpdateCommand}. */
        short UPDATE = 43;

        /** Message type for {@link BuildIndexCommand}. */
        short BUILD_INDEX = 44;

        /** Message type for {@link UpdateMinimumActiveTxStartTimeCommand}. */
        short UPDATE_MINIMUM_ACTIVE_TX_TIME_COMMAND = 45;
    }

    /**
     * Messages for disaster recovery process.
     */
    interface DisasterRecoveryMessages {
        /** Message type for {@link LocalPartitionStateMessage}. */
        short LOCAL_PARTITION_STATE = 100;

        /** Message type for {@link LocalPartitionStatesRequest}. */
        short LOCAL_PARTITION_STATE_REQUEST = 101;

        /** Message type for {@link LocalPartitionStatesResponse}. */
        short LOCAL_PARTITION_STATE_RESPONSE = 102;
    }
}
