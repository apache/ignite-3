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

package org.apache.ignite.internal.cli.call.recovery.restart;

import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.rest.client.api.RecoveryApi;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.RestartPartitionsRequest;

/** Call to restart partitions. */
@Singleton
public class RestartPartitionsCall implements Call<RestartPartitionsCallInput, String> {
    private final ApiClientFactory clientFactory;

    public RestartPartitionsCall(ApiClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    @Override
    public DefaultCallOutput<String> execute(RestartPartitionsCallInput input) {
        RecoveryApi client = new RecoveryApi(clientFactory.getClient(input.clusterUrl()));

        RestartPartitionsRequest command = new RestartPartitionsRequest();

        command.setPartitionIds(input.partitionIds());
        command.setNodeNames(input.nodeNames());
        command.setTableName(input.tableName());
        command.setZoneName(input.zoneName());

        try {
            client.restartPartitions(command);

            return DefaultCallOutput.success(
                    "Successfully restarted partitions."
            );
        } catch (ApiException e) {
            return DefaultCallOutput.failure(new IgniteCliApiException(e, input.clusterUrl()));
        }
    }
}
