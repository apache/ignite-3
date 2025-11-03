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

package org.apache.ignite.internal.disaster.system;

import java.io.IOException;
import java.util.List;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.rest.client.api.ClusterManagementApi;
import org.apache.ignite.rest.client.api.RecoveryApi;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.ClusterState;
import org.apache.ignite.rest.client.model.MigrateRequest;
import org.apache.ignite.rest.client.model.ResetClusterRequest;
import org.jetbrains.annotations.Nullable;

/**
 * Used to run system disaster recovery CLI commands.
 */
class SystemDisasterRecoveryClient {
    private static final IgniteLogger LOG = Loggers.forClass(SystemDisasterRecoveryClient.class);

    static void initiateClusterReset(
            String httpHost,
            int httpPort,
            @Nullable Integer metastorageReplicationFactor,
            String... newCmgNodeNames
    ) {
        LOG.info(
                "Initiating cluster reset via {}:{}, new CMG {}, metastorage replication factor {}",
                httpHost,
                httpPort,
                List.of(newCmgNodeNames),
                metastorageReplicationFactor
        );

        try {
            RecoveryApi recoveryApi = new RecoveryApi(createApiClient(httpHost, httpPort));
            ResetClusterRequest resetRequest = new ResetClusterRequest()
                    .cmgNodeNames(List.of(newCmgNodeNames))
                    .metastorageReplicationFactor(metastorageReplicationFactor);

            recoveryApi.resetCluster(resetRequest);
        } catch (ApiException e) {
            if (e.getCause() instanceof IOException) {
                LOG.info("Node has gone, this most probably means that cluster repair is initiated and the node restarts.");
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    static void initiateMigration(String oldHttpHost, int oldHttpPort, String newHttpHost, int newHttpPort) {
        LOG.info("Initiating migration, old {}:{}, new {}:{}", oldHttpHost, oldHttpPort, newHttpHost, newHttpPort);

        try {
            ClusterManagementApi clusterManagementApi = new ClusterManagementApi(createApiClient(newHttpHost, newHttpPort));
            ClusterState clusterState = clusterManagementApi.clusterState();

            RecoveryApi recoveryApi = new RecoveryApi(createApiClient(oldHttpHost, oldHttpPort));
            MigrateRequest migrateRequest = new MigrateRequest()
                    .cmgNodes(clusterState.getCmgNodes())
                    .metaStorageNodes(clusterState.getMsNodes())
                    .version(clusterState.getIgniteVersion())
                    .clusterId(clusterState.getClusterTag().getClusterId())
                    .clusterName(clusterState.getClusterTag().getClusterName())
                    .formerClusterIds(clusterState.getFormerClusterIds());

            recoveryApi.migrate(migrateRequest);
        } catch (ApiException e) {
            if (e.getCause() instanceof IOException) {
                LOG.info("Node has gone, this most probably means that migration is initiated and the node restarts.");
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private static ApiClient createApiClient(String httpHost, int httpPort) {
        return new ApiClient().setBasePath("http://" + httpHost + ":" + httpPort);
    }
}
