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

import java.io.Serializable;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.network.ClusterNode;

/**
 * A lease representation in memory.
 * The real lease is stored in Metastorage.
 */
public class Lease implements Serializable {
    /** A node that holds a lease until {@code stopLeas}. */
    private ClusterNode leaseholder;

    /** Timestamp to expiration the lease. */
    private HybridTimestamp leaseExpirationTime;

    /**
     * Default constructor.
     */
    public Lease() {
        this(null, null);
    }

    /**
     * The constructor.
     *
     * @param leaseholder Lease holder.
     * @param leaseExpirationTime Lease expiration timestamp.
     */
    public Lease(ClusterNode leaseholder, HybridTimestamp leaseExpirationTime) {
        this.leaseholder = leaseholder;
        this.leaseExpirationTime = leaseExpirationTime;
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
     * Sets a leaseholder node.
     *
     * @param leaseholder Leaseholder node.
     */
    public void setLeaseholder(ClusterNode leaseholder) {
        this.leaseholder = leaseholder;
    }

    /**
     * Gets a lease expiration timestamp.
     *
     * @return Lease expiration timestamp or {@code null} if nothing holds the lease.
     */
    public HybridTimestamp getLeaseExpirationTime() {
        return leaseExpirationTime;
    }

    /**
     * Sets a lease expiration timestamp.
     *
     * @param leaseExpirationTime Lease expiration timestamp.
     */
    public void setLeaseExpirationTime(HybridTimestamp leaseExpirationTime) {
        this.leaseExpirationTime = leaseExpirationTime;
    }

    @Override
    public String toString() {
        return "Lease {"
                + "leaseholder=" + leaseholder
                + ", leaseExpirationTime=" + leaseExpirationTime
                + '}';
    }
}
