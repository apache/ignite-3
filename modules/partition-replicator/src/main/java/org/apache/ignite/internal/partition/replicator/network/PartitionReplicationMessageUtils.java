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

import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestampToLong;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toTablePartitionIdMessage;

import java.util.ArrayList;
import org.apache.ignite.internal.partition.replicator.network.raft.TxMetaMessage;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.TablePartitionIdMessage;
import org.apache.ignite.internal.tx.TxMeta;

/** Class that can contain useful constants and methods for working with messages from {@link PartitionReplicationMessageGroup}. */
public class PartitionReplicationMessageUtils {
    /**
     * Converts to a network message.
     *
     * @param partitionReplicationMessagesFactory Partition replication messages factory.
     * @param replicaMessagesFactory Replica messages factory.
     * @param txMeta Transaction meta.
     * @return New instance of network message.
     */
    public static TxMetaMessage toTxMetaMessage(
            PartitionReplicationMessagesFactory partitionReplicationMessagesFactory,
            ReplicaMessagesFactory replicaMessagesFactory,
            TxMeta txMeta
    ) {
        var enlistedPartitionMessages = new ArrayList<TablePartitionIdMessage>(txMeta.enlistedPartitions().size());

        for (TablePartitionId enlistedPartition : txMeta.enlistedPartitions()) {
            enlistedPartitionMessages.add(toTablePartitionIdMessage(replicaMessagesFactory, enlistedPartition));
        }

        return partitionReplicationMessagesFactory.txMetaMessage()
                .txStateInt(txMeta.txState().ordinal())
                .commitTimestampLong(hybridTimestampToLong(txMeta.commitTimestamp()))
                .enlistedPartitions(enlistedPartitionMessages)
                .build();
    }
}
