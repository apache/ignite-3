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

package org.apache.ignite.client.handler;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.affinity.TokenizedAssignments;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;

/**
 * Fake placement driver.
 */
public class FakePlacementDriver extends AbstractEventProducer<PrimaryReplicaEvent, PrimaryReplicaEventParameters>
        implements PlacementDriver {
    private final int partitions;

    private final List<ReplicaMeta> primaryReplicas;

    private boolean returnError;

    public FakePlacementDriver(int partitions) {
        this.partitions = partitions;
        primaryReplicas = new ArrayList<>(Collections.nCopies(partitions, getReplicaMeta("s", HybridTimestamp.MIN_VALUE.longValue())));
    }

    public void returnError(boolean returnError) {
        this.returnError = returnError;
    }

    /**
     * Sets all primary replicas.
     */
    public void setReplicas(List<String> replicas, int tableId, long leaseStartTime) {
        assert replicas.size() == partitions;

        for (int partition = 0; partition < replicas.size(); partition++) {
            String replica = replicas.get(partition);
            updateReplica(replica, tableId, partition, leaseStartTime);
        }
    }

    /**
     * Sets primary replica for the given partition.
     */
    public void updateReplica(String replica, int tableId, int partition, long leaseStartTime) {
        primaryReplicas.set(partition, getReplicaMeta(replica, leaseStartTime));
        TablePartitionId groupId = new TablePartitionId(tableId, partition);

        PrimaryReplicaEventParameters params = new PrimaryReplicaEventParameters(
                0,
                groupId,
                replica,
                replica,
                HybridTimestamp.hybridTimestamp(leaseStartTime)
        );

        fireEvent(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, params);
    }

    @Override
    public CompletableFuture<ReplicaMeta> awaitPrimaryReplica(ReplicationGroupId groupId, HybridTimestamp timestamp, long timeout,
            TimeUnit unit) {
        TablePartitionId id = (TablePartitionId) groupId;

        return returnError
                ? failedFuture(new RuntimeException("FakePlacementDriver expected error"))
                : CompletableFuture.completedFuture(primaryReplicas.get(id.partitionId()));
    }

    @Override
    public CompletableFuture<ReplicaMeta> getPrimaryReplica(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp) {
        return awaitPrimaryReplica(replicationGroupId, timestamp, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<Void> previousPrimaryExpired(ReplicationGroupId grpId) {
        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<TokenizedAssignments> getAssignments(
            ReplicationGroupId replicationGroupId,
            HybridTimestamp clusterTimeToAwait
    ) {
        return failedFuture(new UnsupportedOperationException("getAssignments() is not supported in FakePlacementDriver yet."));
    }

    private static ReplicaMeta getReplicaMeta(String leaseholder, long leaseStartTime) {
        //noinspection serial
        return new ReplicaMeta() {
            @Override
            public String getLeaseholder() {
                return leaseholder;
            }

            @Override
            public String getLeaseholderId() {
                return leaseholder;
            }

            @Override
            public HybridTimestamp getStartTime() {
                return HybridTimestamp.hybridTimestamp(leaseStartTime);
            }

            @Override
            public HybridTimestamp getExpirationTime() {
                return HybridTimestamp.MAX_VALUE;
            }
        };
    }
}
