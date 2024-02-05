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

import static org.apache.ignite.internal.placementdriver.negotiation.LeaseAgreement.UNDEFINED_AGREEMENT;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.placementdriver.LeaseUpdater;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessageResponse;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessagesFactory;
import org.apache.ignite.internal.replicator.ReplicationGroupId;

/**
 * This class negotiates a lease with leaseholder. If the lease is negotiated, it is ready available to accept.
 */
public class LeaseNegotiator {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(LeaseNegotiator.class);

    private static final PlacementDriverMessagesFactory PLACEMENT_DRIVER_MESSAGES_FACTORY = new PlacementDriverMessagesFactory();

    /** Leases ready to accept. */
    private final Map<ReplicationGroupId, LeaseAgreement> leaseToNegotiate;

    /** Cluster service. */
    private final ClusterService clusterService;

    /**
     * The constructor.
     *
     * @param clusterService Cluster service.
     */
    public LeaseNegotiator(ClusterService clusterService) {
        this.clusterService = clusterService;

        this.leaseToNegotiate = new ConcurrentHashMap<>();
    }

    /**
     * Tries negotiating a lease with its leaseholder.
     * The negotiation will achieve after the method is invoked. Use {@link #negotiated(ReplicationGroupId)} to check a result.
     *
     * @param lease Lease to negotiate.
     * @param force If the flag is true, the process tries to insist of apply the lease.
     */
    public void negotiate(Lease lease, boolean force) {
        var fut = new CompletableFuture<LeaseGrantedMessageResponse>();

        ReplicationGroupId groupId = lease.replicationGroupId();

        leaseToNegotiate.put(groupId, new LeaseAgreement(lease, fut));

        long leaseInterval = lease.getExpirationTime().getPhysical() - lease.getStartTime().getPhysical();

        clusterService.messagingService().invoke(
                        lease.getLeaseholder(),
                        PLACEMENT_DRIVER_MESSAGES_FACTORY.leaseGrantedMessage()
                                .groupId(groupId)
                                .leaseStartTimeLong(lease.getStartTime().longValue())
                                .leaseExpirationTimeLong(lease.getExpirationTime().longValue())
                                .force(force)
                                .build(),
                        leaseInterval)
                .whenComplete((msg, throwable) -> {
                    if (throwable == null) {
                        assert msg instanceof LeaseGrantedMessageResponse : "Message type is unexpected [type="
                                + msg.getClass().getSimpleName() + ']';
                    } else if (!(unwrapCause(throwable) instanceof NodeStoppingException)) {
                        LOG.warn("Lease was not negotiated due to exception [lease={}]", throwable, lease);
                    }

                    LeaseGrantedMessageResponse response = (LeaseGrantedMessageResponse) msg;

                    fut.complete(response);

                    triggerToRenewLeases();
                });
    }

    /**
     * Gets a lease agreement or {@code null} if the agreement has not formed yet.
     *
     * @param groupId Replication group id.
     * @return Lease agreement.
     */
    public LeaseAgreement negotiated(ReplicationGroupId groupId) {
        LeaseAgreement agreement = leaseToNegotiate.getOrDefault(groupId, UNDEFINED_AGREEMENT);

        return agreement;
    }

    /**
     * Removes lease from list to negotiate.
     *
     * @param groupId Lease to expire.
     */
    public void onLeaseRemoved(ReplicationGroupId groupId) {
        leaseToNegotiate.remove(groupId);
    }

    /**
     * Triggers to renew leases forcibly. The method wakes up the monitor of {@link LeaseUpdater}.
     */
    private void triggerToRenewLeases() {
        //TODO: IGNITE-18879 Implement lease maintenance.
    }
}
