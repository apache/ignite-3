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

package org.apache.ignite.internal.datareplication.network.disaster;

import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Marshallable;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.datareplication.network.PartitionReplicationMessageGroup.DisasterRecoveryMessages;
import org.apache.ignite.internal.datareplication.network.command.TablePartitionIdMessage;

/**
 * Local partition state message, has partition ID, state and last committed log index.
 */
@Transferable(DisasterRecoveryMessages.LOCAL_PARTITION_STATE)
public interface LocalPartitionStateMessage extends NetworkMessage {
    /** Partition ID. */
    TablePartitionIdMessage partitionId();

    /** Calculated state of the partition. */
    @Marshallable
    LocalPartitionStateEnum state();

    /** Index of the last received log entry for this partition. */
    long logIndex();
}
