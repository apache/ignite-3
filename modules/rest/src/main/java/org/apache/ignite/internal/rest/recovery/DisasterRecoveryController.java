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

package org.apache.ignite.internal.rest.recovery;

import static java.util.Collections.emptySet;
import static java.util.Comparator.comparing;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.annotation.Controller;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.internal.rest.api.recovery.DisasterRecoveryApi;
import org.apache.ignite.internal.rest.api.recovery.GlobalZonePartitionStateResponse;
import org.apache.ignite.internal.rest.api.recovery.GlobalZonePartitionStatesResponse;
import org.apache.ignite.internal.rest.api.recovery.LocalZonePartitionStateResponse;
import org.apache.ignite.internal.rest.api.recovery.LocalZonePartitionStatesResponse;
import org.apache.ignite.internal.rest.api.recovery.ResetZonePartitionsRequest;
import org.apache.ignite.internal.rest.api.recovery.RestartZonePartitionsRequest;
import org.apache.ignite.internal.rest.exception.handler.IgniteInternalExceptionHandler;
import org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryManager;
import org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionState;
import org.apache.ignite.internal.table.distributed.disaster.LocalPartitionState;
import org.apache.ignite.internal.table.distributed.disaster.LocalPartitionStateByNode;

/**
 * Disaster recovery controller.
 */
@Controller("/management/v1/recovery/")
@Requires(classes = IgniteInternalExceptionHandler.class)
public class DisasterRecoveryController implements DisasterRecoveryApi, ResourceHolder {
    private DisasterRecoveryManager disasterRecoveryManager;

    public DisasterRecoveryController(DisasterRecoveryManager disasterRecoveryManager) {
        this.disasterRecoveryManager = disasterRecoveryManager;
    }

    @Override
    public CompletableFuture<Void> resetZonePartitions(ResetZonePartitionsRequest command) {
        return disasterRecoveryManager.resetPartitions(command.zoneName(), command.partitionIds());
    }

    @Override
    public CompletableFuture<Void> restartZonePartitions(RestartZonePartitionsRequest command) {
        return disasterRecoveryManager.restartPartitions(command.nodeNames(), command.zoneName(), command.partitionIds());
    }

    @Override
    public CompletableFuture<Void> restartZonePartitionsWithCleanup(RestartZonePartitionsRequest command) {
        return disasterRecoveryManager.restartPartitionsWithCleanup(command.nodeNames(), command.zoneName(), command.partitionIds());
    }

    @Override
    public CompletableFuture<LocalZonePartitionStatesResponse> getZoneLocalPartitionStates(
            Optional<Set<String>> zoneNames,
            Optional<Set<String>> nodeNames,
            Optional<Set<Integer>> partitionIds
    ) {
        return disasterRecoveryManager.localPartitionStates(
                zoneNames.orElse(emptySet()),
                nodeNames.orElse(emptySet()),
                partitionIds.orElse(emptySet())
        ).thenApply(DisasterRecoveryController::convertLocalZoneStates);
    }

    @Override
    public CompletableFuture<GlobalZonePartitionStatesResponse> getZoneGlobalPartitionStates(
            Optional<Set<String>> zoneNames,
            Optional<Set<Integer>> partitionIds
    ) {
        return disasterRecoveryManager.globalPartitionStates(
                zoneNames.orElse(emptySet()),
                partitionIds.orElse(emptySet())
        ).thenApply(DisasterRecoveryController::convertGlobalZoneStates);
    }

    private static LocalZonePartitionStatesResponse convertLocalZoneStates(Map<ZonePartitionId, LocalPartitionStateByNode> localStates) {
        List<LocalZonePartitionStateResponse> states = new ArrayList<>();

        for (LocalPartitionStateByNode map : localStates.values()) {
            for (Entry<String, LocalPartitionState> entry : map.entrySet()) {
                String nodeName = entry.getKey();
                LocalPartitionState state = entry.getValue();

                states.add(new LocalZonePartitionStateResponse(
                        nodeName,
                        state.zoneName,
                        state.partitionId,
                        state.state.name(),
                        state.estimatedRows
                ));
            }
        }

        // Sort the output conveniently.
        states.sort(comparing(LocalZonePartitionStateResponse::partitionId)
                .thenComparing(LocalZonePartitionStateResponse::nodeName));

        return new LocalZonePartitionStatesResponse(states);
    }

    private static GlobalZonePartitionStatesResponse convertGlobalZoneStates(Map<ZonePartitionId, GlobalPartitionState> globalStates) {
        List<GlobalZonePartitionStateResponse> states = new ArrayList<>();

        for (GlobalPartitionState state : globalStates.values()) {
            states.add(new GlobalZonePartitionStateResponse(
                    state.zoneName,
                    state.partitionId,
                    state.state.name()
            ));
        }

        // Sort the output conveniently.
        states.sort(comparing(GlobalZonePartitionStateResponse::partitionId));

        return new GlobalZonePartitionStatesResponse(states);
    }

    @Override
    public void cleanResources() {
        disasterRecoveryManager = null;
    }
}
