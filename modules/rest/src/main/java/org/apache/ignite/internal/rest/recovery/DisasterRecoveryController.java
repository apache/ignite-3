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
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.components.NodeProperties;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.internal.rest.api.recovery.DisasterRecoveryApi;
import org.apache.ignite.internal.rest.api.recovery.GlobalPartitionStateResponse;
import org.apache.ignite.internal.rest.api.recovery.GlobalPartitionStatesResponse;
import org.apache.ignite.internal.rest.api.recovery.GlobalZonePartitionStateResponse;
import org.apache.ignite.internal.rest.api.recovery.GlobalZonePartitionStatesResponse;
import org.apache.ignite.internal.rest.api.recovery.LocalPartitionStateResponse;
import org.apache.ignite.internal.rest.api.recovery.LocalPartitionStatesResponse;
import org.apache.ignite.internal.rest.api.recovery.LocalZonePartitionStateResponse;
import org.apache.ignite.internal.rest.api.recovery.LocalZonePartitionStatesResponse;
import org.apache.ignite.internal.rest.api.recovery.ResetPartitionsRequest;
import org.apache.ignite.internal.rest.api.recovery.ResetZonePartitionsRequest;
import org.apache.ignite.internal.rest.api.recovery.RestartPartitionsRequest;
import org.apache.ignite.internal.rest.api.recovery.RestartZonePartitionsRequest;
import org.apache.ignite.internal.rest.exception.handler.IgniteInternalExceptionHandler;
import org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryManager;
import org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionState;
import org.apache.ignite.internal.table.distributed.disaster.GlobalTablePartitionState;
import org.apache.ignite.internal.table.distributed.disaster.LocalPartitionState;
import org.apache.ignite.internal.table.distributed.disaster.LocalPartitionStateByNode;
import org.apache.ignite.internal.table.distributed.disaster.LocalTablePartitionState;
import org.apache.ignite.internal.table.distributed.disaster.LocalTablePartitionStateByNode;
import org.apache.ignite.table.QualifiedName;

/**
 * Disaster recovery controller.
 */
@Controller("/management/v1/recovery/")
@Requires(classes = IgniteInternalExceptionHandler.class)
public class DisasterRecoveryController implements DisasterRecoveryApi, ResourceHolder {
    private DisasterRecoveryManager disasterRecoveryManager;
    private NodeProperties nodeProperties;

    public DisasterRecoveryController(DisasterRecoveryManager disasterRecoveryManager, NodeProperties nodeProperties) {
        this.disasterRecoveryManager = disasterRecoveryManager;
        this.nodeProperties = nodeProperties;
    }

    @Override
    public CompletableFuture<LocalPartitionStatesResponse> getLocalPartitionStates(
            Optional<Set<String>> zoneNames,
            Optional<Set<String>> nodeNames,
            Optional<Set<Integer>> partitionIds
    ) {
        return disasterRecoveryManager.localTablePartitionStates(
                        zoneNames.orElse(Set.of()),
                        nodeNames.orElse(Set.of()),
                        partitionIds.orElse(Set.of())
                )
                .thenApply(DisasterRecoveryController::convertLocalTableStates);
    }

    @Override
    public CompletableFuture<GlobalPartitionStatesResponse> getGlobalPartitionStates(
            Optional<Set<String>> zoneNames,
            Optional<Set<Integer>> partitionIds
    ) {
        return disasterRecoveryManager.globalTablePartitionStates(
                        zoneNames.orElse(Set.of()),
                        partitionIds.orElse(Set.of())
                )
                .thenApply(DisasterRecoveryController::convertGlobalStates);
    }

    @Override
    public CompletableFuture<Void> resetPartitions(@Body ResetPartitionsRequest command) {
        if (nodeProperties.colocationEnabled()) {
            return resetZonePartitions(new ResetZonePartitionsRequest(
                    command.zoneName(),
                    command.partitionIds()
            ));
        }

        QualifiedName tableName = QualifiedName.parse(command.tableName());
        return disasterRecoveryManager.resetTablePartitions(
                command.zoneName(),
                tableName.schemaName(),
                tableName.objectName(),
                command.partitionIds()
        );
    }

    @Override
    public CompletableFuture<Void> restartPartitions(@Body RestartPartitionsRequest command) {
        if (nodeProperties.colocationEnabled()) {
            return restartZonePartitions(new RestartZonePartitionsRequest(
                    command.nodeNames(),
                    command.zoneName(),
                    command.partitionIds()
            ));
        }
        QualifiedName tableName = QualifiedName.parse(command.tableName());
        return disasterRecoveryManager.restartTablePartitions(
                command.nodeNames(),
                command.zoneName(),
                tableName.schemaName(),
                tableName.objectName(),
                command.partitionIds()
        );
    }

    @Override
    public CompletableFuture<Void> restartPartitionsWithCleanup(@Body RestartPartitionsRequest command) {
        if (nodeProperties.colocationEnabled()) {
            return restartZonePartitionsWithCleanup(new RestartZonePartitionsRequest(
                    command.nodeNames(),
                    command.zoneName(),
                    command.partitionIds()
            ));
        }

        QualifiedName tableName = QualifiedName.parse(command.tableName());

        return disasterRecoveryManager.restartTablePartitionsWithCleanup(
                command.nodeNames(),
                command.zoneName(),
                tableName.schemaName(),
                tableName.objectName(),
                command.partitionIds()
        );
    }

    @Override
    public CompletableFuture<Void> resetZonePartitions(ResetZonePartitionsRequest command) {
        checkColocationEnabled();

        return disasterRecoveryManager.resetPartitions(command.zoneName(), command.partitionIds());
    }

    @Override
    public CompletableFuture<Void> restartZonePartitions(RestartZonePartitionsRequest command) {
        checkColocationEnabled();

        return disasterRecoveryManager.restartPartitions(command.nodeNames(), command.zoneName(), command.partitionIds());
    }

    @Override
    public CompletableFuture<Void> restartZonePartitionsWithCleanup(RestartZonePartitionsRequest command) {
        checkColocationEnabled();

        return disasterRecoveryManager.restartPartitionsWithCleanup(command.nodeNames(), command.zoneName(), command.partitionIds());
    }

    @Override
    public CompletableFuture<LocalZonePartitionStatesResponse> getZoneLocalPartitionStates(
            Optional<Set<String>> zoneNames,
            Optional<Set<String>> nodeNames,
            Optional<Set<Integer>> partitionIds
    ) {
        checkColocationEnabled();

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
        checkColocationEnabled();

        return disasterRecoveryManager.globalPartitionStates(
                zoneNames.orElse(emptySet()),
                partitionIds.orElse(emptySet())
        ).thenApply(DisasterRecoveryController::convertGlobalZoneStates);
    }

    private static LocalPartitionStatesResponse convertLocalTableStates(Map<TablePartitionId, LocalTablePartitionStateByNode> localStates) {
        List<LocalPartitionStateResponse> states = new ArrayList<>();

        for (LocalTablePartitionStateByNode map : localStates.values()) {
            for (Entry<String, LocalTablePartitionState> entry : map.entrySet()) {
                String nodeName = entry.getKey();
                LocalTablePartitionState state = entry.getValue();

                states.add(new LocalPartitionStateResponse(
                        nodeName,
                        state.zoneName,
                        state.schemaName,
                        state.tableId,
                        state.tableName,
                        state.partitionId,
                        state.state.name(),
                        state.estimatedRows
                ));
            }
        }

        return createLocalPartitionStatesResponse(states);
    }

    private static LocalPartitionStatesResponse createLocalPartitionStatesResponse(List<LocalPartitionStateResponse> states) {
        // Sort the output conveniently.
        states.sort(comparing(LocalPartitionStateResponse::schemaName)
                .thenComparing(LocalPartitionStateResponse::tableName)
                .thenComparingInt(LocalPartitionStateResponse::partitionId)
                .thenComparing(LocalPartitionStateResponse::nodeName));

        return new LocalPartitionStatesResponse(states);
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

    private static GlobalPartitionStatesResponse convertGlobalStates(Map<TablePartitionId, GlobalTablePartitionState> globalStates) {
        List<GlobalPartitionStateResponse> states = new ArrayList<>();

        for (GlobalTablePartitionState state : globalStates.values()) {
            states.add(new GlobalPartitionStateResponse(
                    state.zoneName,
                    state.schemaName,
                    state.tableId,
                    state.tableName,
                    state.partitionId,
                    state.state.name()
            ));
        }

        return createGlobalPartitionStatesResponse(states);
    }

    private static GlobalPartitionStatesResponse createGlobalPartitionStatesResponse(List<GlobalPartitionStateResponse> states) {
        // Sort the output conveniently.
        states.sort(comparing(GlobalPartitionStateResponse::schemaName)
                .thenComparing(GlobalPartitionStateResponse::tableName)
                .thenComparingInt(GlobalPartitionStateResponse::partitionId));

        return new GlobalPartitionStatesResponse(states);
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

    private void checkColocationEnabled() {
        if (!nodeProperties.colocationEnabled()) {
            throw new UnsupportedOperationException("This method is unsupported when colocation is disabled.");
        }
    }

    @Override
    public void cleanResources() {
        disasterRecoveryManager = null;
        nodeProperties = null;
    }
}
