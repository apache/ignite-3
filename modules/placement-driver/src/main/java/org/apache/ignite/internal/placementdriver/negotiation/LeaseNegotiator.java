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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.placementdriver.negotiation.LeaseAgreement.UNDEFINED_AGREEMENT;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.lang.ComponentStoppingException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteThrottledLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.message.LeaseGrantedMessageResponse;
import org.apache.ignite.internal.placementdriver.message.PlacementDriverMessagesFactory;
import org.apache.ignite.internal.replicator.ReplicationGroupId;

/**
 * This class negotiates a lease with leaseholder. If the lease is negotiated, it is ready available to accept.
 */
public class LeaseNegotiator {
    private static final PlacementDriverMessagesFactory PLACEMENT_DRIVER_MESSAGES_FACTORY = new PlacementDriverMessagesFactory();

    /** The logger. */
    private final IgniteThrottledLogger log;

    /** Lease agreements which are in progress of negotiation. */
    private final Map<ReplicationGroupId, LeaseAgreement> leaseToNegotiate = new ConcurrentHashMap<>();

    /** Cluster service. */
    private final ClusterService clusterService;

    /**
     * The constructor.
     *
     * @param clusterService Cluster service.
     * @param throttledLogExecutor Executor to clean up the throttled logger cache.
     */
    public LeaseNegotiator(
            ClusterService clusterService,
            Executor throttledLogExecutor
    ) {
        this.clusterService = clusterService;

        log = Loggers.toThrottledLogger(Loggers.forClass(LeaseNegotiator.class), throttledLogExecutor);
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
                        if (!hasCause(throwable, NodeStoppingException.class, ComponentStoppingException.class)) {
                            log.warn(
                                    "Lease was not negotiated due to exception",
                                    () -> format("Lease was not negotiated due to exception [lease={}]", lease),
                                    throwable
                            );
                        }

                        leaseToNegotiate.remove(agreement.groupId(), agreement);

                        agreement.cancel();
                    }
                });
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
