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

package org.apache.ignite.internal.rest.recovery.system;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.annotation.Controller;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.ClusterTag;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.disaster.system.SystemDisasterRecoveryManager;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.internal.rest.api.recovery.system.MigrateRequest;
import org.apache.ignite.internal.rest.api.recovery.system.ResetClusterRequest;
import org.apache.ignite.internal.rest.api.recovery.system.SystemDisasterRecoveryApi;
import org.apache.ignite.internal.rest.exception.handler.ClusterResetExceptionHandler;
import org.apache.ignite.internal.rest.exception.handler.IgniteInternalExceptionHandler;
import org.apache.ignite.internal.rest.exception.handler.MigrateExceptionHandler;

/**
 * Controller for system groups disaster recovery.
 */
@Controller("/management/v1/recovery/cluster/")
@Requires(classes = {
        ClusterResetExceptionHandler.class,
        MigrateExceptionHandler.class,
        IgniteInternalExceptionHandler.class
})
public class SystemDisasterRecoveryController implements SystemDisasterRecoveryApi, ResourceHolder {
    private static final IgniteLogger LOG = Loggers.forClass(SystemDisasterRecoveryController.class);

    private SystemDisasterRecoveryManager systemDisasterRecoveryManager;

    private final CmgMessagesFactory cmgMessagesFactory = new CmgMessagesFactory();

    public SystemDisasterRecoveryController(SystemDisasterRecoveryManager systemDisasterRecoveryManager) {
        this.systemDisasterRecoveryManager = systemDisasterRecoveryManager;
    }

    @Override
    public CompletableFuture<Void> reset(ResetClusterRequest command) {
        LOG.info("Reset command is {}", command);

        if (command.metastorageRepairRequested()) {
            return systemDisasterRecoveryManager.resetClusterRepairingMetastorage(
                    command.cmgNodeNames(),
                    command.metastorageReplicationFactor()
            );
        } else {
            return systemDisasterRecoveryManager.resetCluster(command.cmgNodeNames());
        }
    }

    @Override
    public CompletableFuture<Void> migrate(MigrateRequest command) {
        LOG.info("Migrate command is {}", command);

        return systemDisasterRecoveryManager.migrate(migrateRequestToClusterState(command));
    }

    private ClusterState migrateRequestToClusterState(MigrateRequest command) {
        List<UUID> formerClusterIds = command.formerClusterIds();

        return cmgMessagesFactory.clusterState()
                .cmgNodes(Set.copyOf(command.cmgNodes()))
                .metaStorageNodes(Set.copyOf(command.metaStorageNodes()))
                .version(command.version())
                .clusterTag(ClusterTag.clusterTag(cmgMessagesFactory, command.clusterName(), command.clusterId()))
                .formerClusterIds(formerClusterIds == null ? null : List.copyOf(formerClusterIds))
                .build();
    }

    @Override
    public void cleanResources() {
        systemDisasterRecoveryManager = null;
    }
}
