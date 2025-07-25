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
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.lang.ComponentStoppingException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.UnresolvableConsistentIdException;
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

    /**
     * There is a time lag between choosing a candidate leaseholder and negotiation start due to meta storage invocation.
     * In some cases we can report the connection problems that happened because of topology change during this time. This timeout
     * is used to avoid multiple reports on the same leaseholder.
     */
    private static final long LEASEHOLDER_ISSUE_STORE_TIME_MILLISECONDS = 10_000;

    private static final PlacementDriverMessagesFactory PLACEMENT_DRIVER_MESSAGES_FACTORY = new PlacementDriverMessagesFactory();

    /** Lease agreements which are in progress of negotiation. */
    private final Map<ReplicationGroupId, LeaseAgreement> leaseToNegotiate = new ConcurrentHashMap<>();

    /**
     * Map of leaseholders to timestamps when issues related to them happened.
     * This is used to avoid flooding the logs with messages about leaseholder issues.
     */
    private final Map<String, Long> leaseholderIssueTime = new ConcurrentHashMap<>();

    /** Cluster service. */
    private final ClusterService clusterService;

    /**
     * The constructor.
     *
     * @param clusterService Cluster service.
     */
    public LeaseNegotiator(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    /**
     * Tries negotiating a lease with its leaseholder.
     * The negotiation will achieve after the method is invoked. Use {@link #getAndRemoveIfReady(ReplicationGroupId)} to check a result.
     *
     * @param agreement Lease agreement to negotiate.
     */
    public void negotiate(LeaseAgreement agreement) {
        Lease lease = agreement.getLease();

        long leaseInterval = lease.getExpirationTime().getPhysical() - lease.getStartTime().getPhysical();

        leaseToNegotiate.put(agreement.groupId(), agreement);

        clusterService.messagingService().invoke(
                        lease.getLeaseholder(),
                        PLACEMENT_DRIVER_MESSAGES_FACTORY.leaseGrantedMessage()
                                .groupId(agreement.groupId())
                                .leaseStartTime(lease.getStartTime())
                                .leaseExpirationTime(lease.getExpirationTime())
                                .force(agreement.forced())
                                .build(),
                        leaseInterval)
                .whenComplete((msg, throwable) -> {
                    if (throwable == null) {
                        assert msg instanceof LeaseGrantedMessageResponse : "Message type is unexpected [type="
                                + msg.getClass().getSimpleName() + ']';

                        LeaseGrantedMessageResponse response = (LeaseGrantedMessageResponse) msg;

                        agreement.onResponse(response);
                    } else {
                        if (shouldReportError(throwable, lease)) {
                            LOG.warn("Lease was not negotiated due to exception [lease={}]", throwable, lease);
                        }

                        leaseToNegotiate.remove(agreement.groupId(), agreement);

                        agreement.cancel();
                    }
                });
    }

    private void cleanupLeaseholderIssueTime() {
        long now = System.currentTimeMillis();

        leaseholderIssueTime.entrySet().removeIf(entry -> now - entry.getValue() > LEASEHOLDER_ISSUE_STORE_TIME_MILLISECONDS);
    }

    private boolean shouldReportError(Throwable th, Lease lease) {
        if (hasCause(th, NodeStoppingException.class) || hasCause(th, ComponentStoppingException.class)) {
            return false;
        }

        if (isConnectionProblem(th)) {
            Long lastIssueTime = leaseholderIssueTime.get(lease.getLeaseholder());

            if (lastIssueTime != null) {
                return false;
            } else {
                long now = System.currentTimeMillis();
                leaseholderIssueTime.put(lease.getLeaseholder(), now);
                return true;
            }
        }

        return true;
    }

    private static boolean isConnectionProblem(Throwable th) {
        return hasCause(th, UnresolvableConsistentIdException.class);
    }

    /**
     * Called when a new round of lease updates is started.
     */
    public void onNewLeaseUpdateRound() {
        cleanupLeaseholderIssueTime();
    }

    /**
     * Gets a lease agreement or {@link LeaseAgreement#UNDEFINED_AGREEMENT} if the process of agreement is not started yet. Removes
     * the agreement from the map if it is ready.
     *
     * @param groupId Replication group id.
     * @return Lease agreement.
     */
    public LeaseAgreement getAndRemoveIfReady(ReplicationGroupId groupId) {
        LeaseAgreement[] res = new LeaseAgreement[1];

        leaseToNegotiate.compute(groupId, (k, v) -> {
            res[0] = v;

            return v != null && v.ready() ? null : v;
        });

        return res[0] == null ? UNDEFINED_AGREEMENT : res[0];
    }

    /**
     * Removes lease from list to negotiate.
     *
     * @param groupId Lease to expire.
     */
    public void cancelAgreement(ReplicationGroupId groupId) {
        LeaseAgreement agreement = leaseToNegotiate.remove(groupId);

        if (agreement != null) {
            agreement.cancel();
        }
    }
}
