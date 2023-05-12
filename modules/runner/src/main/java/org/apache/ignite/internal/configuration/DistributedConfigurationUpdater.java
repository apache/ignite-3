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

package org.apache.ignite.internal.configuration;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.configuration.presentation.ConfigurationPresentation;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;

/**
 * Updater is responsible for applying changes to the cluster configuration when it's ready.
 */
public class DistributedConfigurationUpdater implements IgniteComponent {

    private static final IgniteLogger LOG = Loggers.forClass(DistributedConfigurationUpdater.class);

    private final ClusterManagementGroupManager cmgMgr;

    private final ConfigurationPresentation<String> presentation;

    public DistributedConfigurationUpdater(ClusterManagementGroupManager cmgMgr, ConfigurationPresentation<String> presentation) {
        this.cmgMgr = cmgMgr;
        this.presentation = presentation;
    }

    @Override
    public void start() {
        cmgMgr.clusterConfigurationToUpdate()
                .thenApply(action -> {
                    if (action.configuration() != null) {
                        presentation.update(action.configuration());
                    }
                    return action;
                })
                .whenComplete((action, e) -> {
                    CompletableFuture<Void> result = new CompletableFuture<>();
                    if (e != null) {
                        LOG.error("Failed to update the distributed configuration", e);
                        result.completeExceptionally(e);
                    } else {
                        LOG.info("Successfully updated the distributed configuration");
                        result.complete(null);
                    }
                    action.nextAction().apply(result);
                });
    }

    @Override
    public void stop() throws Exception {
    }
}
