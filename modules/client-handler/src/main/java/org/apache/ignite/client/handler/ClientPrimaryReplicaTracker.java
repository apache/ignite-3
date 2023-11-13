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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.jetbrains.annotations.Nullable;
import org.apache.ignite.internal.placementdriver.PlacementDriver;

/**
 * Primary partition replica tracker. Shared by all instances of {@link ClientInboundMessageHandler}.
 * Tracks primary replica for every partition.
 */
public class ClientPrimaryReplicaTracker {
    private static final List<String> PENDING = Collections.emptyList();

    private final ConcurrentHashMap<Integer, List<String>> primaryReplicas = new ConcurrentHashMap<>();

    /** Update counter for all tables. */
    private final AtomicLong updateCount = new AtomicLong();

    private final PlacementDriver placementDriver;

    public ClientPrimaryReplicaTracker(PlacementDriver placementDriver) {
        assert placementDriver != null;

        this.placementDriver = placementDriver;

        EventListener<PrimaryReplicaEventParameters> listener = (eventParameters, err) -> {
            if (err != null || !(eventParameters.groupId() instanceof TablePartitionId)) {
                return CompletableFuture.completedFuture(null);
            }

            TablePartitionId tablePartitionId = (TablePartitionId) eventParameters.groupId();
            primaryReplicas.computeIfPresent(tablePartitionId.tableId(), (ignored, oldVal) -> {
                if (oldVal.isEmpty()) {
                    // Initial value is not set yet.
                    return oldVal;
                }

                assert oldVal.size() > tablePartitionId.partitionId() : "replicas.size() > tablePartitionId.partitionId()";
                oldVal.set(tablePartitionId.partitionId(), eventParameters.leaseholder());

                return oldVal;
            });

            updateCount.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        };

        // TODO: Unsubscribe somewhere.
        placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, listener);
    }

    // TODO: For every tracked table, maintain an "assignment version" that is incremented every time a replica changes.
    public @Nullable List<String> primaryReplicas(int tableId) {
        List<String> replicas = primaryReplicas.computeIfAbsent(tableId, this::init);

        return replicas == PENDING ? null : replicas;
    }

    public long updateCount() {
        return updateCount.get();
    }

    private List<String> init(Integer tableId) {
        // TODO: Where do we get partition count?
        // TODO: Request initial assignment.

        return PENDING;
    }
}
