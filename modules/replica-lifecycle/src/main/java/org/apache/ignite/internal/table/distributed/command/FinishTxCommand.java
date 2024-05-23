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

package org.apache.ignite.internal.table.distributed.command;

import static org.apache.ignite.internal.hlc.HybridTimestamp.nullableHybridTimestamp;

import java.util.List;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.table.distributed.PartitionReplicationMessageGroup;
import org.jetbrains.annotations.Nullable;

/**
 * State machine command to finish a transaction on a commit or a rollback.
 */
@Transferable(PartitionReplicationMessageGroup.Commands.FINISH_TX)
public interface FinishTxCommand extends PartitionCommand {
    /**
     * Returns a commit or a rollback state.
     */
    boolean commit();

    /**
     * Returns a transaction commit timestamp.
     */
    long commitTimestampLong();

    /**
     * Returns a transaction commit timestamp.
     */
    default @Nullable HybridTimestamp commitTimestamp() {
        return nullableHybridTimestamp(commitTimestampLong());
    }

    /**
     * Returns an ordered replication groups ids.
     */
    List<TablePartitionIdMessage> partitionIds();

}
