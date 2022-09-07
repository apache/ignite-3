/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.List;
import java.util.UUID;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.raft.client.WriteCommand;

/**
 * State machine command to finish the transaction on commit or rollback.
 */
public class FinishTxCommand extends PartitionCommand implements WriteCommand {
    /**
     * Commit or rollback state.
     */
    private final boolean commit;

    /**
     * Transaction commit timestamp.
     */
    private final HybridTimestamp commitTimestamp;

    /**
     * Replication groups ids.
     */
    private final List<String> replicationGroupIds;

    /**
     * The constructor.
     *
     * @param txId The txId.
     * @param commit Commit or rollback state {@code True} to commit.
     * @param commitTimestamp Transaction commit timestamp.
     * @param replicationGroupIds Set of replication groups ids.
     */
    public FinishTxCommand(UUID txId, boolean commit, HybridTimestamp commitTimestamp, List<String> replicationGroupIds) {
        super(txId);
        this.commit = commit;
        this.commitTimestamp = commitTimestamp;
        this.replicationGroupIds = replicationGroupIds;
    }

    /**
     * Returns commit or rollback state.
     *
     * @return Commit or rollback state.
     */
    public boolean commit() {
        return commit;
    }

    /**
     * Returns transaction commit timestamp.
     *
     * @return Transaction commit timestamp.
     */
    public HybridTimestamp commitTimestamp() {
        return commitTimestamp;
    }

    /**
     * Returns ordered replication groups ids.
     *
     * @return Ordered replication groups ids.
     */
    public List<String> replicationGroupIds() {
        return replicationGroupIds;
    }
}
