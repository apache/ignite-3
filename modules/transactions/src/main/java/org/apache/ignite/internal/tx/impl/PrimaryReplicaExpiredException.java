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

package org.apache.ignite.internal.tx.impl;

import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_PRIMARY_REPLICA_EXPIRED_ERR;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.jetbrains.annotations.Nullable;

/** Unchecked exception that is thrown when primary replica has expired. */
public class PrimaryReplicaExpiredException extends IgniteInternalException {
    /**
     * The constructor.
     *
     * @param groupId Replication group id.
     * @param expectedEnlistmentConsistencyToken Expected enlistment consistency token.
     * @param commitTimestamp Commit timestamp.
     * @param currentPrimaryReplica Current primary replica
     */
    public PrimaryReplicaExpiredException(
            ReplicationGroupId groupId,
            Long expectedEnlistmentConsistencyToken,
            @Nullable HybridTimestamp commitTimestamp,
            @Nullable ReplicaMeta currentPrimaryReplica
    ) {
        super(
                TX_PRIMARY_REPLICA_EXPIRED_ERR,
                "Primary replica has expired, transaction will be rolled back: [groupId = {},"
                        + " expected enlistment consistency token = {}, commit timestamp = {},"
                        + " current primary replica = {}]",
                groupId, expectedEnlistmentConsistencyToken, commitTimestamp, currentPrimaryReplica
        );
    }
}
