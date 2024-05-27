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

import static java.util.Comparator.comparing;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.internal.rest.api.recovery.DisasterRecoveryApi;
import org.apache.ignite.internal.rest.api.recovery.GlobalPartitionStateResponse;
import org.apache.ignite.internal.rest.api.recovery.GlobalPartitionStatesResponse;
import org.apache.ignite.internal.rest.api.recovery.LocalPartitionStateResponse;
import org.apache.ignite.internal.rest.api.recovery.LocalPartitionStatesResponse;
import org.apache.ignite.internal.rest.api.recovery.ResetPartitionsRequest;
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
    public CompletableFuture<LocalPartitionStatesResponse> getLocalPartitionStates(
            Optional<Set<String>> zoneNames,
            Optional<Set<String>> nodeNames,
            Optional<Set<Integer>> partitionIds
    ) {
        return disasterRecoveryManager.localPartitionStates(
                        zoneNames.orElse(Set.of()),
                        nodeNames.orElse(Set.of()),
                        partitionIds.orElse(Set.of())
                )
                .thenApply(DisasterRecoveryController::convertLocalStates);
    }

    @Override
    public CompletableFuture<GlobalPartitionStatesResponse> getGlobalPartitionStates(
            Optional<Set<String>> zoneNames,
            Optional<Set<Integer>> partitionIds
    ) {
        return disasterRecoveryManager.globalPartitionStates(
                        zoneNames.orElse(Set.of()),
                        partitionIds.orElse(Set.of())
                )
                .thenApply(DisasterRecoveryController::convertGlobalStates);
    }

    @Override
    public CompletableFuture<Void> resetPartitions(@Body ResetPartitionsRequest command) {
        return disasterRecoveryManager.resetPartitions(
                command.zoneName(),
                command.tableName(),
                command.partitionIds()
        );
    }

    private static LocalPartitionStatesResponse convertLocalStates(Map<TablePartitionId, LocalPartitionStateByNode> localStates) {
        List<LocalPartitionStateResponse> states = new ArrayList<>();

        for (LocalPartitionStateByNode map : localStates.values()) {
            for (Entry<String, LocalPartitionState> entry : map.entrySet()) {
                String nodeName = entry.getKey();
                LocalPartitionState state = entry.getValue();

                states.add(new LocalPartitionStateResponse(
                        state.partitionId,
                        state.tableName,
                        state.zoneName,
                        nodeName,
                        state.state.name()
                ));
            }
        }

        // Sort the output conveniently.
        states.sort(comparing(LocalPartitionStateResponse::tableName)
                .thenComparingInt(LocalPartitionStateResponse::partitionId)
                .thenComparing(LocalPartitionStateResponse::nodeName));

        return new LocalPartitionStatesResponse(states);
    }

    private static GlobalPartitionStatesResponse convertGlobalStates(Map<TablePartitionId, GlobalPartitionState> globalStates) {
        List<GlobalPartitionStateResponse> states = new ArrayList<>();

        for (GlobalPartitionState state : globalStates.values()) {
            states.add(new GlobalPartitionStateResponse(
                    state.partitionId,
                    state.tableName,
                    state.zoneName,
                    state.state.name()
            ));
        }

        // Sort the output conveniently.
        states.sort(comparing(GlobalPartitionStateResponse::tableName)
                .thenComparingInt(GlobalPartitionStateResponse::partitionId));

        return new GlobalPartitionStatesResponse(states);
    }

    @Override
    public void cleanResources() {
        disasterRecoveryManager = null;
    }
}
