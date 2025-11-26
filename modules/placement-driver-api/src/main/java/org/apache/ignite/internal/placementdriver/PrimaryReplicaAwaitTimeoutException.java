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

package org.apache.ignite.internal.placementdriver;

import static org.apache.ignite.lang.ErrorGroups.PlacementDriver.PRIMARY_REPLICA_AWAIT_TIMEOUT_ERR;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.tx.RetriableTransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * The exception is thrown when a primary replica await process has times out.
 */
public class PrimaryReplicaAwaitTimeoutException extends IgniteInternalException implements RetriableTransactionException {
    private static final long serialVersionUID = -1450288033816499192L;

    /**
     * The constructor.
     *
     * @param replicationGroupId Replication group id.
     * @param referenceTimestamp Timestamp reference value.
     * @param cause Cause exception.
     */
    public PrimaryReplicaAwaitTimeoutException(
            ReplicationGroupId replicationGroupId,
            HybridTimestamp referenceTimestamp,
            @Nullable ReplicaMeta currentLease,
            Throwable cause
    ) {
        super(
                PRIMARY_REPLICA_AWAIT_TIMEOUT_ERR,
                "The primary replica await timed out [replicationGroupId={}, referenceTimestamp={}, currentLease={}]",
                cause,
                replicationGroupId, referenceTimestamp, currentLease
        );
    }
}
