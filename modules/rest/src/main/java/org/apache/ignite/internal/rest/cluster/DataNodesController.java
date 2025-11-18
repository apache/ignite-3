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

package org.apache.ignite.internal.rest.cluster;

import io.micronaut.http.annotation.Controller;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.internal.rest.api.cluster.zone.datanodes.DataNodesApi;

/**
 * Distributed zones data nodes controller.
 */
@Controller("/management/v1/zones")
public class DataNodesController implements DataNodesApi, ResourceHolder {
    private DistributionZoneManager distributionZoneManager;

    public DataNodesController(DistributionZoneManager distributionZoneManager) {
        this.distributionZoneManager = distributionZoneManager;
    }

    @Override
    public CompletableFuture<Set<String>> getDataNodesForZone(String zoneName) {
        return distributionZoneManager.currentDataNodes(zoneName);
    }

    @Override
    public CompletableFuture<Void> resetDataNodesForZone(String zoneName) {
        return distributionZoneManager.recalculateDataNodes(zoneName);
    }

    @Override
    public CompletableFuture<Void> resetDataNodesForZones(Optional<Set<String>> zoneNames) {
        return distributionZoneManager.recalculateDataNodes(zoneNames.orElse(Set.of()));
    }

    @Override
    public void cleanResources() {
        distributionZoneManager = null;
    }
}
