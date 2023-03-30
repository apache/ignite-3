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

import static org.apache.ignite.internal.placementdriver.LeaseUpdater.LEASE_PERIOD;
import static org.apache.ignite.internal.placementdriver.conciliation.LeaseAgreement.UNDEFINED_AGREEMENT;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.placementdriver.LeaseUpdater;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessageResponse;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessagesFactory;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.network.ClusterService;

/**
 * This class conciliates a lease with leaseholder. If the lease is conciliated, it is ready available to accept.
 */
public class LeaseConciliator {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(LeaseConciliator.class);

    /** Placement driver messages factory. */
    private static final PlacementDriverMessagesFactory PLACEMENT_DRIVER_MESSAGES_FACTORY = new PlacementDriverMessagesFactory();

    /** Leases ready to accept. */
    private final Map<ReplicationGroupId, LeaseAgreement> leaseToConciliate;

    /** Cluster service. */
    private final ClusterService clusterService;

    /**
     * The constructor.
     *
     * @param clusterService Cluster service.
     */
    public LeaseConciliator(ClusterService clusterService) {
        this.clusterService = clusterService;

        this.leaseToConciliate = new ConcurrentHashMap<>();
    }

    /**
     * Tries conciliating a lease with its leaseholder.
     * The conciliation will achieve after the method is invoked. Use {@link #conciliated(ReplicationGroupId)} to check a result.
     *
     * @param groupId Lease replication group id.
     * @param lease Lease to conciliate.
     * @param force If the flag is true, the process tries to insist of apply the lease.
     */
    public void conciliate(ReplicationGroupId groupId, Lease lease, boolean force) {
        clusterService.messagingService().invoke(
                        lease.getLeaseholder().name(),
                        PLACEMENT_DRIVER_MESSAGES_FACTORY.leaseGrantedMessage()
                                .groupId(groupId)
                                .leaseStartTime(lease.getStartTime())
                                .leaseExpirationTime(lease.getExpirationTime())
                                .force(force)
                                .build(),
                        LEASE_PERIOD)
                .whenComplete((msg, throwable) -> {
                    if (throwable != null) {
                        LOG.warn("Lease was not conciliated due to exception [lease={}]", throwable, lease);
                    } else {
                        assert msg instanceof LeaseGrantedMessageResponse : "Message type is unexpected [type="
                                + msg.getClass().getSimpleName() + ']';

                        var resp = (LeaseGrantedMessageResponse) msg;

                        leaseToConciliate.put(groupId, new LeaseAgreement(lease, resp.accepted(), resp.redirectProposal()));

                        triggerToRenewLeases();
                    }
                });
    }

    /**
     * Gets a lease agreement or {@code null} if the agreement has not formed yet.
     *
     * @param groupId Replication group id.
     * @return Lease agreement.
     */
    public LeaseAgreement conciliated(ReplicationGroupId groupId) {
        LeaseAgreement agreement = leaseToConciliate.getOrDefault(groupId, UNDEFINED_AGREEMENT);

        return agreement;
    }

    /**
     * Removes lease from list to conciliate.
     *
     * @param groupId Lease to expire.
     */
    public void onLeaseRemoved(ReplicationGroupId groupId) {
        leaseToConciliate.remove(groupId);
    }

    /**
     * Triggers to renew leases forcibly. The method wakes up the monitor of {@link LeaseUpdater}.
     */
    private void triggerToRenewLeases() {
        //TODO: IGNITE-18879 Implement lease maintenance.
    }
}
