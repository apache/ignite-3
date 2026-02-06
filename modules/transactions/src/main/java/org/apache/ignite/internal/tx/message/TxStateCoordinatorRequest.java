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

package org.apache.ignite.internal.tx.message;

import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.replicator.message.ZonePartitionIdMessage;
import org.jetbrains.annotations.Nullable;

/**
 * Transaction state request.
 */
@Transferable(TxMessageGroup.TX_STATE_COORDINATOR_REQUEST)
public interface TxStateCoordinatorRequest extends NetworkMessage {
    UUID txId();

    HybridTimestamp readTimestamp();

    /**
     * If this request is caused by write intent resolution, the message may contain the group id and current consistency token of
     * the partition where the write intent is located (sender partition). This is the current consistency token of the sender partition.
     *
     * @return Current consistency token of the sender partition, or {@code null} if the request is not caused by write intent resolution.
     */
    @Nullable Long senderCurrentConsistencyToken();

    /**
     * If this request is caused by write intent resolution, the message may contain the group id and current consistency token of
     * the partition where the write intent is located (sender partition). This is the group id of the sender partition.
     *
     * @return Group id of the sender partition, or {@code null} if the request is not caused by write intent resolution.
     */
    @Nullable ZonePartitionIdMessage senderGroupId();
}
