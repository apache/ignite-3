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

package org.apache.ignite.internal.partition.replicator;

import static java.util.Objects.requireNonNull;

import org.apache.ignite.internal.partition.replicator.network.replication.ReadOnlyReplicaRequest;
import org.apache.ignite.internal.replicator.message.PrimaryReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaSafeTimeSyncRequest;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Represents replica primacy info. Contains the following information:
 *
 * <ul>
 *     <li>{@code leaseStartTime} - the moment when the replica became primary (only filled for {@link PrimaryReplicaRequest}s)</li>
 *     <li>{@code isPrimary} - whether this node currently hosts the primary (only filled for {@link ReadOnlyReplicaRequest}s
 *     and {@link ReplicaSafeTimeSyncRequest}s)</li>
 * </ul>
 */
public class ReplicaPrimacy {
    private static final ReplicaPrimacy EMPTY = new ReplicaPrimacy(null, null);

    private final @Nullable Long leaseStartTime;
    private final @Nullable Boolean isPrimary;

    private ReplicaPrimacy(@Nullable Long leaseStartTime, @Nullable Boolean isPrimary) {
        this.leaseStartTime = leaseStartTime;
        this.isPrimary = isPrimary;
    }

    /**
     * Creates an instance representing no primacy information.
     */
    public static ReplicaPrimacy empty() {
        return EMPTY;
    }

    /**
     * Creates an instance representing information about the primary replica held by this node.
     */
    @VisibleForTesting
    public static ReplicaPrimacy forPrimaryReplicaRequest(long leaseStartTime) {
        return new ReplicaPrimacy(leaseStartTime, null);
    }

    /**
     * Creates an instance representing information about whether this node currently holds the primary.
     */
    @VisibleForTesting
    public static ReplicaPrimacy forIsPrimary(boolean isPrimary) {
        return new ReplicaPrimacy(null, isPrimary);
    }

    /**
     * Returns lease start time; throws exception if not present.
     */
    public long leaseStartTime() {
        return requireNonNull(leaseStartTime);
    }

    /**
     * Whether this node currently hosts the primary; throws if this information is absent.
     */
    public boolean isPrimary() {
        return requireNonNull(isPrimary);
    }
}
