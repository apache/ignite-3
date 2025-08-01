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

package org.apache.ignite.internal.cli.call.recovery.cluster;

import jakarta.inject.Singleton;
import java.io.IOException;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.rest.client.api.RecoveryApi;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.ResetClusterRequest;

/** Call to reset cluster (that is, initiate CMG/Metastorage group repair). */
@Singleton
public class ResetClusterCall implements Call<ResetClusterCallInput, String> {
    private final ApiClientFactory clientFactory;

    public ResetClusterCall(ApiClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    @Override
    public DefaultCallOutput<String> execute(ResetClusterCallInput input) {
        RecoveryApi client = new RecoveryApi(clientFactory.getClient(input.clusterUrl()));

        ResetClusterRequest command = new ResetClusterRequest();

        command.setCmgNodeNames(input.cmgNodeNames());
        command.setMetastorageReplicationFactor(input.metastorageReplicationFactor());

        try {
            client.resetCluster(command);

            return DefaultCallOutput.success("Successfully initiated cluster repair.");
        } catch (ApiException e) {
            if (e.getCause() instanceof IOException) {
                return DefaultCallOutput.success("Node has gone, this most probably means that cluster repair is initiated and "
                        + "the node restarts.");
            }

            return DefaultCallOutput.failure(new IgniteCliApiException(e, input.clusterUrl()));
        }
    }
}
