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

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.findAny;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessageResponse;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.jetbrains.annotations.Nullable;

/**
 * The agreement is formed from {@link LeaseGrantedMessageResponse}.
 */
public class LeaseAgreement {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(LeaseAgreement.class);

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
        this.responseFut = requireNonNull(remoteNodeResponseFuture);
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
     * Whether the agreement is declined (ready but not accepted).
     *
     * @return Whether the agreement is declined (ready but not accepted).
     */
    public boolean isDeclined() {
        return ready() && !isAccepted();
    }

    /**
     * The property matches to {@link LeaseGrantedMessageResponse#redirectProposal()}.
     * This property is available only when the agreement is ready (look at {@link #ready()}).
     *
     * @return Node id to propose a lease.
     */
    @Nullable
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
        return responseFut.isDone();
    }

    /**
     * Check the validity of the agreement in the current logical topology and group assignments. If the suggested leaseholder
     * has left topology or not included into the current assignments, the agreement is broken.
     *
     * @param groupId Group id.
     * @param currentTopologySnapshot Current topology snapshot.
     * @param assignments Assignments.
     */
    public void checkValid(
            ReplicationGroupId groupId,
            @Nullable LogicalTopologySnapshot currentTopologySnapshot,
            Set<Assignment> assignments
    ) {
        if (ready()) {
            return;
        }

        if (findAny(assignments, a -> a.consistentId().equals(lease.getLeaseholder())).isEmpty()) {
            LOG.info("Lease was not negotiated because the node is not included into the group assignments anymore [node={}, group={}, "
                    + "assignments={}].", lease.getLeaseholder(), lease, assignments);

            responseFut.complete(null);
        } else if (currentTopologySnapshot != null) {
            Set<String> nodeIds = currentTopologySnapshot.nodes().stream().map(LogicalNode::id).collect(toSet());

            if (!nodeIds.contains(lease.getLeaseholderId())) {
                LOG.info("Lease was not negotiated because the node has left the logical topology [node={}, nodeId={}, group={}]",
                        lease.getLeaseholder(), lease.getLeaseholderId(), groupId);

                responseFut.complete(null);
            }
        }
    }
}
