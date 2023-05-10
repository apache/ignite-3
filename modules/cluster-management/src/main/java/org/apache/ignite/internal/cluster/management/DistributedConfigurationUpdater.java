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

package org.apache.ignite.internal.cluster.management;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.configuration.presentation.ConfigurationPresentation;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.lang.NodeStoppingException;

/**
 * Updater is responsible for applying changes to the cluster configuration when it's ready.
 */
public class DistributedConfigurationUpdater implements IgniteComponent {

    private static final IgniteLogger LOG = Loggers.forClass(DistributedConfigurationUpdater.class);

    private final CompletableFuture<ConfigurationPresentation<String>> clusterCfgPresentation = new CompletableFuture<>();

    public void setDistributedConfigurationPresentation(ConfigurationPresentation<String> presentation) {
        clusterCfgPresentation.complete(presentation);
    }

    /**
     * Applies changes to the cluster configuration when {@link DistributedConfigurationUpdater#clusterCfgPresentation}
     * is complete.
     *
     * @param configurationToApply Cluster configuration that should be applied.
     * @return Future that will be completed when cluster configuration is updated.
     */
    public CompletableFuture<Void> updateConfiguration(String configurationToApply) {
        return clusterCfgPresentation.thenCompose(presentation -> presentation.update(configurationToApply))
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.error("Unable to update cluster configuration", e);
                    } else {
                        LOG.info("Cluster configuration updated successfully");
                    }
                });
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() throws Exception {
        clusterCfgPresentation.completeExceptionally(new NodeStoppingException("Component is stopped."));
    }
}
