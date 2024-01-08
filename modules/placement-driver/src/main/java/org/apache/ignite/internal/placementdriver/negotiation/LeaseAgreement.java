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

package org.apache.ignite.internal.placementdriver.negotiation;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessageResponse;

/**
 * The agreement is formed from {@link LeaseGrantedMessageResponse}.
 */
public class LeaseAgreement {
    /**
     * The agreement, which has not try negotiating yet. We assume that it is {@link #ready()} and not {@link #isAccepted()}
     * which allows both initiation and retries of negotiation.
     */
    public static final LeaseAgreement UNDEFINED_AGREEMENT = new LeaseAgreement(null, nullCompletedFuture());

    /** Lease. */
    private final Lease lease;

    /** Future to {@link LeaseGrantedMessageResponse} response. */
    private final CompletableFuture<LeaseGrantedMessageResponse> responseFut;

    /**
     * The constructor.
     *
     * @param lease Lease.
     * @param remoteNodeResponseFuture The future of response from the remote node which is negotiating the agreement.
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
        if (!ready()) {
            return false;
        }

        LeaseGrantedMessageResponse resp = responseFut.join();

        if (resp != null) {
            return resp.accepted();
        }

        return false;
    }

    /**
     * The property matches to {@link LeaseGrantedMessageResponse#redirectProposal()}.
     * This property is available only when the agreement is ready (look at {@link #ready()}).
     *
     * @return Node id to propose a lease.
     */
    public String getRedirectTo() {
        assert ready() : "The method should be invoked only after the agreement is ready";

        LeaseGrantedMessageResponse resp = responseFut.join();

        return resp != null ? resp.redirectProposal() : null;
    }

    /**
     * Returns true if the agreement is negotiated, false otherwise.
     *
     * @return True if a response of the agreement has been received, false otherwise.
     */
    public boolean ready() {
        return responseFut != null && responseFut.isDone();
    }
}
