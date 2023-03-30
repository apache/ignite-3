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

package org.apache.ignite.internal.placementdriver.conciliation;

import org.apache.ignite.internal.placementdriver.leases.Lease;

/**
 * The agreement is formed from {@link org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessageResponse}.
 */
public class LeaseAgreement {
    /** The agreement, which has not try conciliating yet. */
    public static final LeaseAgreement UNDEFINED_AGREEMENT = new LeaseAgreement(null, false);

    /** Lease. */
    private final Lease lease;

    /** Accepted flag. */
    private final boolean accepted;

    /** A node that should agree with a lease or {@code null} if there is not a preference node. */
    private final String redirectTo;

    /**
     * The constructor.
     *
     * @param lease Lease.
     * @param accepted If this flag is true the lease is accepted by the leaseholder, otherwise false.
     * @param redirectTo A node that should agree with a lease.
     */
    public LeaseAgreement(Lease lease, boolean accepted, String redirectTo) {
        assert redirectTo == null || !accepted : "Lease accepted by leaseholder, but the node proposes to redirect the lease [";

        this.lease = lease;
        this.accepted = accepted;
        this.redirectTo = redirectTo;
    }

    /**
     * Creates a new agreement.
     *
     * @param lease Lease.
     * @param accepted If this flag is true the lease is accepted by the leaseholder, otherwise false.
     */
    public LeaseAgreement(Lease lease, boolean accepted) {
        this(lease, accepted, null);
    }

    /**
     * Gets a lease about which the leaseholder was notified.
     *
     * @return Lease.
     */
    public Lease getLease() {
        return lease;
    }

    /**
     * Gets a accepted flag. The flag is true, when the lease is accepted by leaseholder.
     *
     * @return Accepted flag.
     */
    public boolean isAccepted() {
        return accepted;
    }

    /**
     * The property is {@code null} if the lease is accepted and not {@code null}  if the leaseholder does not apply the lease and proposes
     * the other node.
     *
     * @return Node id to propose a lease.
     */
    public String getRedirectTo() {
        return redirectTo;
    }
}
