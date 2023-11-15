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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
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

    private final List<String> primaryReplicas;

    public FakePlacementDriver(int partitions) {
        this.partitions = partitions;
        primaryReplicas = new ArrayList<>(Collections.nCopies(partitions, "s"));
    }

    /**
     * Sets all primary replicas.
     */
    public void setReplicas(List<String> replicas, int tableId) {
        assert replicas.size() == partitions;

        for (int partition = 0; partition < replicas.size(); partition++) {
            String replica = replicas.get(partition);
            updateReplica(replica, tableId, partition);
        }
    }

    /**
     * Sets primary replica for the given partition.
     */
    public void updateReplica(String replica, int tableId, int partition) {
        primaryReplicas.set(partition, replica);
        TablePartitionId groupId = new TablePartitionId(tableId, partition);

        PrimaryReplicaEventParameters params = new PrimaryReplicaEventParameters(
                0, groupId, replica, new HybridTimestamp(System.currentTimeMillis(), 0));

        fireEvent(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, params);
    }

    @Override
    public CompletableFuture<ReplicaMeta> awaitPrimaryReplica(ReplicationGroupId groupId, HybridTimestamp timestamp, long timeout,
            TimeUnit unit) {
        var id = (TablePartitionId) groupId;

        return CompletableFuture.completedFuture(getReplicaMeta(primaryReplicas.get(id.partitionId())));
    }

    @Override
    public CompletableFuture<ReplicaMeta> getPrimaryReplica(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp) {
        return awaitPrimaryReplica(replicationGroupId, timestamp, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<Void> previousPrimaryExpired(ReplicationGroupId grpId) {
        return CompletableFuture.completedFuture(null);
    }

    private static ReplicaMeta getReplicaMeta(String leaseholder) {
        //noinspection serial
        return new ReplicaMeta() {
            @Override
            public String getLeaseholder() {
                return leaseholder;
            }

            @Override
            public String getLeaseholderId() {
                return null;
            }

            @Override
            public HybridTimestamp getStartTime() {
                return HybridTimestamp.MIN_VALUE;
            }

            @Override
            public HybridTimestamp getExpirationTime() {
                return HybridTimestamp.MAX_VALUE;
            }
        };
    }
}
