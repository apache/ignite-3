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

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessageResponse;

/**
 * The agreement is formed from {@link org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessageResponse}.
 */
public class LeaseAgreement {
    /** The agreement, which has not try conciliating yet. */
    public static final LeaseAgreement UNDEFINED_AGREEMENT = new LeaseAgreement(null, completedFuture(null));

    /** Lease. */
    private final Lease lease;

    /** Future to {@link LeaseGrantedMessageResponse} response. */
    private final CompletableFuture<LeaseGrantedMessageResponse> responseFut;

    /**
     * The constructor.
     *
     * @param lease Lease.
     * @param remoteNodeResponseFuture The future of response from the remote node which is conciliating the agreement.
     */
    public LeaseAgreement(Lease lease, CompletableFuture<LeaseGrantedMessageResponse> remoteNodeResponseFuture) {
        this.lease = lease;
        this.responseFut = remoteNodeResponseFuture;
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
        if (!responseFut.isDone()) {
            return false;
        }

        LeaseGrantedMessageResponse resp = responseFut.join();

        if (resp != null) {
            return resp.accepted();
        }

        return false;
    }

    /**
     * The property is {@code null} if the lease is accepted and not {@code null}  if the leaseholder does not apply the lease and proposes
     * the other node.
     *
     * @return Node id to propose a lease.
     */
    public String getRedirectTo() {
        if (!responseFut.isDone()) {
            return null;
        }

        LeaseGrantedMessageResponse resp = responseFut.join();

        if (resp != null) {
            return resp.redirectProposal();
        }

        return null;
    }

    /**
     * Returns true if the agreement is conciliated, false otherwise.
     *
     * @return True if a response of the agreement has been received, false otherwise.
     */
    public boolean ready() {
        return responseFut.isDone();
    }
}
