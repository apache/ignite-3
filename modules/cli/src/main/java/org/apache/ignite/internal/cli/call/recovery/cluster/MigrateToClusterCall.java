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
import org.apache.ignite.rest.client.api.ClusterManagementApi;
import org.apache.ignite.rest.client.api.RecoveryApi;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.ClusterState;
import org.apache.ignite.rest.client.model.MigrateRequest;

/** Call to migrate nodes from old to new cluster. */
@Singleton
public class MigrateToClusterCall implements Call<MigrateToClusterCallInput, String> {
    private final ApiClientFactory clientFactory;

    public MigrateToClusterCall(ApiClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    @Override
    public DefaultCallOutput<String> execute(MigrateToClusterCallInput input) {
        ClusterManagementApi newClusterManagementClient = new ClusterManagementApi(clientFactory.getClient(input.newClusterUrl()));
        RecoveryApi oldRecoveryClient = new RecoveryApi(clientFactory.getClient(input.oldClusterUrl()));

        ClusterState newClusterState;
        try {
            newClusterState = newClusterManagementClient.clusterState();
        } catch (ApiException e) {
            return DefaultCallOutput.failure(new IgniteCliApiException(e, input.newClusterUrl()));
        }

        MigrateRequest command = new MigrateRequest();

        command.setCmgNodes(newClusterState.getCmgNodes());
        command.setMetaStorageNodes(newClusterState.getMsNodes());
        command.setVersion(newClusterState.getIgniteVersion());
        command.setClusterId(newClusterState.getClusterTag().getClusterId());
        command.setClusterName(newClusterState.getClusterTag().getClusterName());
        command.setFormerClusterIds(newClusterState.getFormerClusterIds());

        try {
            oldRecoveryClient.migrate(command);
        } catch (ApiException e) {
            if (e.getCause() instanceof IOException) {
                return DefaultCallOutput.success("Node has gone, this most probably means that migration is initiated and "
                        + "the node restarts.");
            }

            return DefaultCallOutput.failure(new IgniteCliApiException(e, input.oldClusterUrl()));
        }

        return DefaultCallOutput.success("Successfully initiated migration.");
    }
}
