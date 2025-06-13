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

package org.apache.ignite.internal.replicator;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import org.apache.ignite.internal.hlc.HybridTimestamp;

/**
 * Exception thrown when a replica reservation fails.
 */
public class ReplicaReservationFailedException extends RuntimeException {
    public static final long serialVersionUID = 33897490775763932L;

    /**
     * Constructor.
     *
     * @param message Exception message.
     */
    ReplicaReservationFailedException(String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param groupId Replication group ID.
     * @param leaseStartTime Lease start time.
     * @param currentReplicaState Current state of the replica.
     */
    public ReplicaReservationFailedException(ReplicationGroupId groupId, HybridTimestamp leaseStartTime, String currentReplicaState) {
        this(format(
                "Replica reservation failed [groupId={}, leaseStartTime={}, currentReplicaState={}].",
                groupId,
                leaseStartTime,
                currentReplicaState
        ));
    }
}
