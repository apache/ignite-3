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

import static org.apache.ignite.internal.hlc.HybridTimestamp.MAX_VALUE;
import static org.apache.ignite.internal.hlc.HybridTimestamp.MIN_VALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/** Test implementation of the {@link ReplicaMeta}. */
@TestOnly
public class TestReplicaMetaImpl implements ReplicaMeta {
    private static final long serialVersionUID = -382174507405586033L;

    /** A node consistent ID that holds a lease, {@code null} if nothing holds the lease. */
    private final @Nullable String leaseholder;

    /** A node ID that holds a lease, {@code null} if nothing holds the lease. */
    private final @Nullable String leaseholderId;

    /** Lease start timestamp. The timestamp is assigned when the lease created and is not changed when the lease is prolonged. */
    private final HybridTimestamp startTime;

    /** Timestamp to expiration the lease. */
    private final HybridTimestamp expirationTime;

    /**
     * Creates a new primary meta with unbounded period.
     *
     * <p>Notes: Delegates creation to a {@link TestReplicaMetaImpl#TestReplicaMetaImpl(String, String, HybridTimestamp, HybridTimestamp)},
     * where {@code leaseholder} is {@link ClusterNode#name()} and {@code leaseholderId} is {@link ClusterNode#id()}.</p>
     *
     * @param leaseholder Lease holder, {@code null} if nothing holds the lease.
     */
    TestReplicaMetaImpl(@Nullable ClusterNode leaseholder) {
        this(leaseholder, MIN_VALUE, MAX_VALUE);
    }

    /**
     * Creates a new primary meta with unbounded period.
     *
     * @param leaseholder Lease holder consistent ID, {@code null} if nothing holds the lease.
     * @param leaseholderId Lease holder ID, {@code null} if nothing holds the lease.
     */
    public TestReplicaMetaImpl(@Nullable String leaseholder, @Nullable String leaseholderId) {
        this(leaseholder, leaseholderId, MIN_VALUE, MAX_VALUE);
    }

    /**
     * Creates a new primary meta.
     *
     * <p>Notes: Delegates creation to a {@link TestReplicaMetaImpl#TestReplicaMetaImpl(String, String, HybridTimestamp, HybridTimestamp)},
     * where {@code leaseholder} is {@link ClusterNode#name()} and {@code leaseholderId} is {@link ClusterNode#id()}.</p>
     *
     * @param leaseholder Lease holder, {@code null} if nothing holds the lease.
     * @param startTime Start lease timestamp.
     * @param expirationTime Lease expiration timestamp.
     */
    public TestReplicaMetaImpl(@Nullable ClusterNode leaseholder, HybridTimestamp startTime, HybridTimestamp expirationTime) {
        this(
                leaseholder == null ? null : leaseholder.name(),
                leaseholder == null ? null : leaseholder.id(),
                startTime,
                expirationTime
        );
    }

    /**
     * Creates a new primary meta.
     *
     * @param leaseholder Lease holder consistent ID, {@code null} if nothing holds the lease.
     * @param leaseholderId Lease holder ID, {@code null} if nothing holds the lease.
     * @param startTime Start lease timestamp.
     * @param expirationTime Lease expiration timestamp.
     */
    private TestReplicaMetaImpl(
            @Nullable String leaseholder,
            @Nullable String leaseholderId,
            HybridTimestamp startTime,
            HybridTimestamp expirationTime
    ) {
        assertEquals(
                leaseholder == null,
                leaseholderId == null,
                String.format("leaseholder=%s, leaseholderId=%s", leaseholder, leaseholderId)
        );

        this.leaseholder = leaseholder;
        this.leaseholderId = leaseholderId;
        this.startTime = startTime;
        this.expirationTime = expirationTime;
    }

    @Override
    public @Nullable String getLeaseholder() {
        return leaseholder;
    }

    @Override
    public @Nullable String getLeaseholderId() {
        return leaseholderId;
    }

    @Override
    public HybridTimestamp getStartTime() {
        return startTime;
    }

    @Override
    public HybridTimestamp getExpirationTime() {
        return expirationTime;
    }
}
