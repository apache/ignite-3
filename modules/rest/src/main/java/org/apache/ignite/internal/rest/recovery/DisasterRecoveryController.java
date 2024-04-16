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
import static org.apache.ignite.internal.rest.recovery.DisasterRecoveryFactory.DISASTER_RECOVERY_MANAGER_NAME;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.annotation.Controller;
import jakarta.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.rest.api.recovery.DisasterRecoveryApi;
import org.apache.ignite.internal.rest.api.recovery.GlobalPartitionStateResponse;
import org.apache.ignite.internal.rest.api.recovery.GlobalPartitionStatesResponse;
import org.apache.ignite.internal.rest.api.recovery.LocalPartitionStateResponse;
import org.apache.ignite.internal.rest.api.recovery.LocalPartitionStatesResponse;
import org.apache.ignite.internal.rest.exception.handler.IgniteInternalExceptionHandler;
import org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryManager;
import org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionState;
import org.apache.ignite.internal.table.distributed.disaster.LocalPartitionState;

/**
 * Disaster recovery controller.
 */
@Controller("/management/v1/recovery/")
@Requires(classes = IgniteInternalExceptionHandler.class)
public class DisasterRecoveryController implements DisasterRecoveryApi {
    private final DisasterRecoveryManager disasterRecoveryManager;

    public DisasterRecoveryController(@Named(DISASTER_RECOVERY_MANAGER_NAME) DisasterRecoveryManager disasterRecoveryManager) {
        this.disasterRecoveryManager = disasterRecoveryManager;
    }

    @Override
    public CompletableFuture<LocalPartitionStatesResponse> getLocalPartitionStates() {
        return disasterRecoveryManager.localPartitionStates(null).thenApply(DisasterRecoveryController::convertLocalStates);
    }

    @Override
    public CompletableFuture<LocalPartitionStatesResponse> getLocalPartitionStates(String zoneName) {
        return disasterRecoveryManager.localPartitionStates(zoneName).thenApply(DisasterRecoveryController::convertLocalStates);
    }

    @Override
    public CompletableFuture<GlobalPartitionStatesResponse> getGlobalPartitionStates() {
        return disasterRecoveryManager.globalPartitionStates(null).thenApply(DisasterRecoveryController::convertGlobalStates);
    }

    @Override
    public CompletableFuture<GlobalPartitionStatesResponse> getGlobalPartitionStates(String zoneName) {
        return disasterRecoveryManager.globalPartitionStates(zoneName).thenApply(DisasterRecoveryController::convertGlobalStates);
    }

    private static LocalPartitionStatesResponse convertLocalStates(Map<TablePartitionId, Map<String, LocalPartitionState>> localStates) {
        List<LocalPartitionStateResponse> states = new ArrayList<>();

        for (Entry<TablePartitionId, Map<String, LocalPartitionState>> entry0 : localStates.entrySet()) {
            for (Entry<String, LocalPartitionState> entry1 : entry0.getValue().entrySet()) {
                String nodeName = entry1.getKey();
                LocalPartitionState state = entry1.getValue();

                states.add(new LocalPartitionStateResponse(
                        state.partitionId,
                        state.tableName,
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

        for (Entry<TablePartitionId, GlobalPartitionState> entry : globalStates.entrySet()) {
            GlobalPartitionState state = entry.getValue();

            states.add(new GlobalPartitionStateResponse(
                    state.partitionId,
                    state.tableName,
                    state.state.name()
            ));
        }

        // Sort the output conveniently.
        states.sort(comparing(GlobalPartitionStateResponse::tableName)
                .thenComparingInt(GlobalPartitionStateResponse::partitionId));

        return new GlobalPartitionStatesResponse(states);
    }
}
