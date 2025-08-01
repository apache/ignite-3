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

package org.apache.ignite.internal.placementdriver.leases;

import static org.apache.ignite.internal.hlc.HybridTimestamp.MIN_VALUE;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;

import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * A lease representation in memory.
 * The real lease is stored in Meta storage.
 */
public class Lease implements ReplicaMeta {
    /** Node consistent ID (assigned to a node once), {@code null} if nothing holds the lease. */
    private final @Nullable String leaseholder;

    /** Leaseholder node ID (changes on every node startup), {@code null} if nothing holds the lease. */
    private final @Nullable UUID leaseholderId;

    /** The lease is accepted, when the holder knows about it and applies all related obligations. */
    private final boolean accepted;

    /** Lease start timestamp. The timestamp is assigned when the lease created and is not changed when the lease is prolonged. */
    private final HybridTimestamp startTime;

    /** Timestamp to expiration the lease. */
    private final HybridTimestamp expirationTime;

    /** The lease is available to prolong in the same leaseholder. */
    private final boolean prolongable;

    /** The name of a node that is proposed to be a next leaseholder. This is not null in case when the lease is not prolongable. */
    @Nullable
    private final String proposedCandidate;

    /** ID of replication group. */
    private final ReplicationGroupId replicationGroupId;

    /**
     * Creates a new lease.
     *
     * @param leaseholder Leaseholder node consistent ID (assigned to a node once), {@code null} if nothing holds the lease.
     * @param leaseholderId Leaseholder node ID (changes on every node startup), {@code null} if nothing holds the lease.
     * @param startTime Start lease timestamp.
     * @param leaseExpirationTime Lease expiration timestamp.
     * @param replicationGroupId ID of replication group.
     */
    public Lease(
            @Nullable String leaseholder,
            @Nullable UUID leaseholderId,
            HybridTimestamp startTime,
            HybridTimestamp leaseExpirationTime,
            ReplicationGroupId replicationGroupId
    ) {
        this(leaseholder, leaseholderId, startTime, leaseExpirationTime, true, false, null, replicationGroupId);
    }

    /**
     * The constructor.
     *
     * @param leaseholder Leaseholder node consistent ID (assigned to a node once), {@code null} if nothing holds the lease.
     * @param leaseholderId Leaseholder node ID (changes on every node startup), {@code null} if nothing holds the lease.
     * @param startTime Start lease timestamp.
     * @param leaseExpirationTime Lease expiration timestamp.
     * @param prolong Lease is available to prolong.
     * @param accepted The flag is {@code true} when the holder accepted the lease.
     * @param proposedCandidate The name of a node that is proposed to be a next leaseholder. This is not null in case when the lease
     *     is not prolongable.
     * @param replicationGroupId ID of replication group.
     */
    public Lease(
            @Nullable String leaseholder,
            @Nullable UUID leaseholderId,
            HybridTimestamp startTime,
            HybridTimestamp leaseExpirationTime,
            boolean prolong,
            boolean accepted,
            @Nullable String proposedCandidate,
            ReplicationGroupId replicationGroupId
    ) {
        assert (leaseholder == null) == (leaseholderId == null) : "leaseholder=" + leaseholder + ", leaseholderId=" + leaseholderId;

        this.leaseholder = leaseholder;
        this.leaseholderId = leaseholderId;
        this.startTime = startTime;
        this.expirationTime = leaseExpirationTime;
        this.prolongable = prolong;
        this.accepted = accepted;
        this.replicationGroupId = replicationGroupId;
        this.proposedCandidate = proposedCandidate;
    }

    /**
     * Prolongs a lease until new timestamp. Only an accepted lease can be prolonged.
     *
     * @param to The new lease expiration timestamp.
     * @return A new lease which will have the same properties except of expiration timestamp.
     */
    public Lease prolongLease(HybridTimestamp to) {
        assert accepted : "The lease should be accepted by leaseholder before prolongation: [lease=" + this + ", to=" + to + ']';
        assert prolongable : "The lease should be available to prolong: [lease=" + this + ", to=" + to + ']';

        return new Lease(leaseholder, leaseholderId, startTime, to, true, true, null, replicationGroupId);
    }

    /**
     * Accepts the lease.
     *
     * @param to The new lease expiration timestamp.
     * @return A accepted lease.
     */
    public Lease acceptLease(HybridTimestamp to) {
        assert !accepted : "The lease is already accepted: " + this;

        return new Lease(leaseholder, leaseholderId, startTime, to, true, true, null, replicationGroupId);
    }

    /**
     * Denies the lease.
     *
     * @return Denied lease.
     */
    public Lease denyLease(@Nullable String proposedCandidate) {
        HybridTimestamp newExpirationTime = accepted ? expirationTime : hybridTimestamp(startTime.longValue() + 1);

        return new Lease(leaseholder, leaseholderId, startTime, newExpirationTime, false, accepted, proposedCandidate, replicationGroupId);
    }

    @Override
    public @Nullable String getLeaseholder() {
        return leaseholder;
    }

    @Override
    public @Nullable UUID getLeaseholderId() {
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

    /** Returns {@code true} if the lease might be prolonged. */
    public boolean isProlongable() {
        return prolongable;
    }

    /** Returns {@code true} if the lease accepted. */
    public boolean isAccepted() {
        return accepted;
    }

    /** Returns ID of replication group. */
    public ReplicationGroupId replicationGroupId() {
        return replicationGroupId;
    }

    /** The name of a node that is proposed to be a next leaseholder. This is not null in case when the lease is not prolongable. */
    @Nullable
    public String proposedCandidate() {
        return proposedCandidate;
    }

    /**
     * Returns a lease that no one holds and is always expired.
     *
     * @param replicationGroupId Replication group ID.
     */
    public static Lease emptyLease(ReplicationGroupId replicationGroupId) {
        return new Lease(null, null, MIN_VALUE, MIN_VALUE, replicationGroupId);
    }

    @Override
    public String toString() {
        return S.toString(Lease.class, this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Lease other = (Lease) o;
        return accepted == other.accepted && prolongable == other.prolongable
                && Objects.equals(leaseholder, other.leaseholder) && Objects.equals(leaseholderId, other.leaseholderId)
                && Objects.equals(startTime, other.startTime) && Objects.equals(expirationTime, other.expirationTime)
                && Objects.equals(replicationGroupId, other.replicationGroupId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leaseholder, leaseholderId, accepted, startTime, expirationTime, prolongable, replicationGroupId);
    }
}
