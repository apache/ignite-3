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
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.rest.configuration.AuthenticationConfiguration;
import org.apache.ignite.internal.rest.configuration.AuthenticationProviderChange;
import org.apache.ignite.internal.rest.configuration.BasicAuthenticationProviderChange;
import org.apache.ignite.internal.rest.configuration.ClusterRestConfiguration;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.rest.AuthenticationType;
import org.apache.ignite.rest.AuthenticationProviderConfig;
import org.apache.ignite.rest.BasicAuthenticationProviderConfig;
import org.apache.ignite.rest.RestAuthenticationConfig;

/**
 * Updater is responsible for applying changes to the cluster configuration when it's ready.
 */
public class DistributedConfigurationUpdater implements IgniteComponent {

    private static final IgniteLogger LOG = Loggers.forClass(DistributedConfigurationUpdater.class);

    private final CompletableFuture<ClusterRestConfiguration> clusterRestConfigurationFuture = new CompletableFuture<>();

    public void setClusterRestConfiguration(ClusterRestConfiguration clusterRestConfiguration) {
        clusterRestConfigurationFuture.complete(clusterRestConfiguration);
    }

    /**
     * Applies changes to the {@link AuthenticationConfiguration} when
     * {@link DistributedConfigurationUpdater#clusterRestConfigurationFuture} is complete.
     *
     * @param restAuthenticationConfig {@link AuthenticationConfiguration} that should be applied.
     * @return Future that will be completed when {@link AuthenticationConfiguration} is applied.
     */
    public CompletableFuture<Void> updateRestAuthConfiguration(RestAuthenticationConfig restAuthenticationConfig) {
        return clusterRestConfigurationFuture.thenApply(ClusterRestConfiguration::authentication)
                .thenCompose(configuration -> changeAuthConfiguration(configuration, restAuthenticationConfig))
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.error("Unable to change auth configuration", e);
                    }
                });
    }

    private static CompletableFuture<Void> changeAuthConfiguration(AuthenticationConfiguration authConfiguration,
            RestAuthenticationConfig config) {
        return authConfiguration.change(authChange -> {
            authChange.changeProviders(providers -> {
                config.providers().forEach(provider -> {
                    providers.create(provider.name(), cfg -> applyProviderChange(cfg, provider));
                });
            });
            authChange.changeEnabled(config.enabled());
        });
    }

    private static void applyProviderChange(AuthenticationProviderChange change, AuthenticationProviderConfig provider) {
        if (provider.type() == AuthenticationType.BASIC) {
            BasicAuthenticationProviderConfig basicAuthProvider = (BasicAuthenticationProviderConfig) provider;
            change.convert(BasicAuthenticationProviderChange.class)
                    .changeLogin(basicAuthProvider.login())
                    .changePassword(basicAuthProvider.password())
                    .changeName(provider.name());
        } else {
            LOG.error("Couldn't make configuration change for type: ", provider.type());
        }
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() throws Exception {
        clusterRestConfigurationFuture.completeExceptionally(new NodeStoppingException("Component is stopped."));
    }
}
