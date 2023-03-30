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

import java.io.Serializable;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.network.ClusterNode;

/**
 * A lease representation in memory.
 * The real lease is stored in Meta storage.
 */
public class Lease implements Serializable {
    /** The object is used when nothing holds the lease. */
    public static Lease EMPTY_LEASE = new Lease(null, null, null, true);

    /** A node that holds a lease until {@code stopLeas}. */
    private final ClusterNode leaseholder;

    /** The lease is accepted, when the holder knows about it and applies all related obligations. */
    private final boolean accepted;

    /** Lease start timestamp. The timestamp is assigned when the lease created and does not be changed when the lease is prolonged. */
    private final HybridTimestamp startTime;

    /** Timestamp to expiration the lease. */
    private final HybridTimestamp expirationTime;

    /**
     * Creates a new lease.
     *
     * @param leaseholder Lease holder.
     * @param startTime Start lease timestamp.
     * @param leaseExpirationTime Lease expiration timestamp.
     */
    public Lease(ClusterNode leaseholder, HybridTimestamp startTime, HybridTimestamp leaseExpirationTime) {
        this(leaseholder, startTime, leaseExpirationTime, false);
    }

    /**
     * The constructor.
     *
     * @param leaseholder Lease holder.
     * @param startTime Start lease timestamp.
     * @param leaseExpirationTime Lease expiration timestamp.
     * @param accepted The flag is true when the holder accepted the lease, the false otherwise.
     */
    private Lease(ClusterNode leaseholder, HybridTimestamp startTime, HybridTimestamp leaseExpirationTime, boolean accepted) {
        this.leaseholder = leaseholder;
        this.expirationTime = leaseExpirationTime;
        this.startTime = startTime;
        this.accepted = accepted;
    }

    /**
     * Prolongs a lease to until a new timestamp. Only an accepted lease available to prolong.
     *
     * @param to The new lease expiration timestamp.
     * @return A new lease which will have the same properties except expire timestamp.
     */
    public Lease prolongLease(HybridTimestamp to) {
        assert accepted : "The lease should be accepted by leaseholder before prolong ["
                + "leaseholder=" + leaseholder
                + ", expirationTime=" + expirationTime
                + ", prolongTo=" + to + ']';

        return new Lease(leaseholder, startTime, to, true);
    }

    /**
     * Accepts the lease.
     *
     * @return A accepted lease.
     */
    public Lease acceptLease() {
        assert !accepted : "The lease is already accepted ["
                + "leaseholder=" + leaseholder
                + ", expirationTime=" + expirationTime + ']';

        return new Lease(leaseholder, startTime, expirationTime, true);
    }

    /**
     * Get a leaseholder node.
     *
     * @return Leaseholder or {@code null} if nothing holds the lease.
     */
    public ClusterNode getLeaseholder() {
        return leaseholder;
    }

    /**
     * Gets a lease start timestamp.
     *
     * @return Lease start timestamp.
     */
    public HybridTimestamp getStartTime() {
        return startTime;
    }

    /**
     * Gets a lease expiration timestamp.
     *
     * @return Lease expiration timestamp or {@code null} if nothing holds the lease.
     */
    public HybridTimestamp getExpirationTime() {
        return expirationTime;
    }

    /**
     * Gets accepted flag.
     *
     * @return True if the lease accepted, false otherwise.
     */
    public boolean isAccepted() {
        return accepted;
    }

    @Override
    public String toString() {
        return "Lease{"
                + "leaseholder=" + leaseholder
                + ", accepted=" + accepted
                + ", startTime=" + startTime
                + ", expirationTime=" + expirationTime
                + '}';
    }
}
