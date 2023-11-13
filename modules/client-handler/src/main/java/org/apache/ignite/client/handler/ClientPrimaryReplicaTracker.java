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
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.jetbrains.annotations.Nullable;
import org.apache.ignite.internal.placementdriver.PlacementDriver;

/**
 * Primary partition replica tracker. Shared by all instances of {@link ClientInboundMessageHandler}.
 * Tracks primary replica for every partition.
 */
public class ClientPrimaryReplicaTracker {
    private final ConcurrentHashMap<String, PrimaryReplicas> primaryReplicas = new ConcurrentHashMap<>();

    private final PlacementDriver placementDriver;

    public ClientPrimaryReplicaTracker(PlacementDriver placementDriver) {
        assert placementDriver != null;

        this.placementDriver = placementDriver;
    }

    // TODO: For every tracked table, maintain an "assignment version" that is incremented every time a replica changes.
    public @Nullable PrimaryReplicas primaryReplicas(String table) {
        // TODO: Start tracking the table on first request.
        return primaryReplicas.computeIfAbsent(table, this::init);
    }

    private PrimaryReplicas init(String table) {
        // TODO: Where do we get partition count?
        EventListener<PrimaryReplicaEventParameters> listener = eventParameters -> {
            if (eventParameters.tableName().equals(table)) {
                primaryReplicas.put(table, new PrimaryReplicas(eventParameters.version(), eventParameters.primaryReplicas()));
            }

            return CompletableFuture.completedFuture(null);
        };
        var event = PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED;
        placementDriver.listen(event, listener);

        return new PrimaryReplicas(0, new ArrayList<>());
    }

    public class PrimaryReplicas {
        public final int version;
        public final List<String> primaryReplicas;

        public PrimaryReplicas(int version, List<String> primaryReplicas) {
            this.version = version;
            this.primaryReplicas = primaryReplicas;
        }
    }
}
