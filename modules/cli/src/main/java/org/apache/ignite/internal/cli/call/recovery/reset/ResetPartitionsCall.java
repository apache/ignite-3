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

package org.apache.ignite.internal.cli.call.recovery.reset;

import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.rest.client.api.RecoveryApi;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.ResetZonePartitionsRequest;

/** Call to reset partitions. */
@Singleton
public class ResetPartitionsCall implements Call<ResetPartitionsCallInput, String> {
    private final ApiClientFactory clientFactory;

    public ResetPartitionsCall(ApiClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    @Override
    public DefaultCallOutput<String> execute(ResetPartitionsCallInput input) {
        RecoveryApi client = new RecoveryApi(clientFactory.getClient(input.clusterUrl()));

        try {
            ResetZonePartitionsRequest command = new ResetZonePartitionsRequest();

            command.setPartitionIds(input.partitionIds());
            command.setZoneName(input.zoneName());

            client.resetZonePartitions(command);

            return DefaultCallOutput.success("Successfully reset partitions.");
        } catch (ApiException e) {
            return DefaultCallOutput.failure(new IgniteCliApiException(e, input.clusterUrl()));
        }
    }
}
