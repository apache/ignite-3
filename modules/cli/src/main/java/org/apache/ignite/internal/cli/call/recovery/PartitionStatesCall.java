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

package org.apache.ignite.internal.cli.call.recovery;

import static java.util.stream.Collectors.toList;

import jakarta.inject.Singleton;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.internal.cli.sql.table.Table;
import org.apache.ignite.rest.client.api.RecoveryApi;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.GlobalPartitionStatesResponse;
import org.apache.ignite.rest.client.model.LocalPartitionStatesResponse;

@Singleton
public class PartitionStatesCall implements Call<PartitionStatesCallInput, Table> {
    private final ApiClientFactory clientFactory;

    private static final List<String> GLOBAL_HEADERS = List.of("Zone name", "Table name", "Partition ID", "State");

    private static final List<String> LOCAL_HEADERS = Stream
            .concat(Stream.of("Node name"), GLOBAL_HEADERS.stream())
            .collect(toList());

    public PartitionStatesCall(ApiClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    @Override
    public DefaultCallOutput<Table> execute(PartitionStatesCallInput input) {
        RecoveryApi client = new RecoveryApi(clientFactory.getClient(input.clusterUrl()));

        List<String> trimmedZoneNames = trim(input.zoneNames());
        List<String> trimmedNodeNames = trim(input.nodeNames());

        try {
            if (input.local()) {
                LocalPartitionStatesResponse localStates = client.getLocalPartitionStates(
                        trimmedZoneNames,
                        trimmedNodeNames,
                        input.partitionIds()
                );

                List<String> content;
                content = localStates.getStates().stream()
                        .flatMap(state -> Stream.of(
                                        state.getNodeName(),
                                        state.getZoneName(),
                                        state.getTableName(),
                                        String.valueOf(state.getPartitionId()),
                                        state.getState()
                                )
                        )
                        .collect(toList());

                return DefaultCallOutput.success(new Table(LOCAL_HEADERS, content));
            } else {
                GlobalPartitionStatesResponse globalStates = client.getGlobalPartitionStates(
                        trimmedZoneNames,
                        input.partitionIds()
                );

                List<String> content = globalStates.getStates().stream()
                        .flatMap(state -> Stream.of(
                                        state.getZoneName(),
                                        state.getTableName(),
                                        String.valueOf(state.getPartitionId()),
                                        state.getState()
                                )
                        )
                        .collect(toList());

                return DefaultCallOutput.success(new Table(GLOBAL_HEADERS, content));
            }
        } catch (ApiException e) {
            return DefaultCallOutput.failure(new IgniteCliApiException(e, input.clusterUrl()));
        }
    }

    private static List<String> trim(List<String> names) {
        return names == null
                ? List.of()
                : names.stream()
                        .map(String::trim)
                        .collect(toList());
    }
}
