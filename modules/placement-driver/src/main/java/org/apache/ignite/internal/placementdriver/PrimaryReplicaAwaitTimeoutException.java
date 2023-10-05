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

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.lang.ErrorGroups;
import org.jetbrains.annotations.Nullable;

/**
 * The exception is thrown when a primary replica await process has times out.
 */
public class PrimaryReplicaAwaitTimeoutException extends IgniteInternalException {

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
            @Nullable Lease currentLease,
            Throwable cause
    ) {
        super(ErrorGroups.PlacementDriver.PRIMARY_REPLICA_AWAIT_TIMEOUT_ERR,
                IgniteStringFormatter.format(
                        "The primary replica await timed out [replicationGroupId={}, referenceTimestamp={}, currentLease={}]",
                        replicationGroupId, referenceTimestamp, currentLease
                ),
                cause);
    }
}
