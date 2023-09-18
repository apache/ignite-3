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

package org.apache.ignite.internal.placementdriver;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.jetbrains.annotations.TestOnly;

/**
 * Test placement driver service that immediately returns unbounded primary replica from both await and get methods for the specified
 * leaseholder.
 */
@TestOnly
public class TestPlacementDriver implements PlacementDriver {
    private final TestReplicaMetaImpl primaryReplica;

    public TestPlacementDriver(String leaseholder) {
        this.primaryReplica = new TestReplicaMetaImpl(leaseholder);
    }

    @Override
    public CompletableFuture<ReplicaMeta> awaitPrimaryReplica(ReplicationGroupId groupId, HybridTimestamp timestamp) {
        return completedFuture(primaryReplica);
    }

    @Override
    public CompletableFuture<ReplicaMeta> getPrimaryReplica(ReplicationGroupId replicationGroupId, HybridTimestamp timestamp) {
        return completedFuture(primaryReplica);
    }

    @Override
    public void listen(PrimaryReplicaEvent evt, EventListener<? extends PrimaryReplicaEventParameters> listener) {
        // TODO: IGNITE-20422 подумать над реализаций и нужна ли
    }

    @Override
    public void removeListener(PrimaryReplicaEvent evt, EventListener<? extends PrimaryReplicaEventParameters> listener) {
        // TODO: IGNITE-20422 подумать над реализаций и нужна ли
    }
}
