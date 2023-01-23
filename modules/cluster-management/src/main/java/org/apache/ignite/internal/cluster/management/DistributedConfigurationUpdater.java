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
import org.apache.ignite.internal.rest.configuration.AuthConfiguration;
import org.apache.ignite.internal.rest.configuration.AuthProviderChange;
import org.apache.ignite.internal.rest.configuration.BasicAuthProviderChange;
import org.apache.ignite.internal.rest.configuration.ClusterRestConfiguration;
import org.apache.ignite.rest.AuthProviderConfig;
import org.apache.ignite.rest.AuthType;
import org.apache.ignite.rest.BasicAuthProviderConfig;
import org.apache.ignite.rest.RestAuthConfig;

/**
 * Updater is responsible for applying changes to the cluster configuration when it's ready.
 */
public class DistributedConfigurationUpdater {

    private static final IgniteLogger LOG = Loggers.forClass(DistributedConfigurationUpdater.class);

    private final CompletableFuture<ClusterRestConfiguration> clusterRestConfigurationFuture = new CompletableFuture<>();

    public void setClusterRestConfiguration(ClusterRestConfiguration clusterRestConfiguration) {
        clusterRestConfigurationFuture.complete(clusterRestConfiguration);
    }

    /** Applies changes to the {@link AuthConfiguration}
     * when {@link DistributedConfigurationUpdater#clusterRestConfigurationFuture} is complete.
     *
     * @param restAuthConfig {@link AuthConfiguration} that should be applied.
     * @return Future that will be completed when {@link AuthConfiguration} is applied.
     * */
    public CompletableFuture<Void> updateRestAuthConfiguration(RestAuthConfig restAuthConfig) {
        return clusterRestConfigurationFuture.thenApply(ClusterRestConfiguration::authConfiguration)
                .thenCompose(authConfiguration -> changeAuthConfiguration(authConfiguration, restAuthConfig))
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.error("Unable to change auth configuration", e);
                    }
                });
    }

    private static CompletableFuture<Void> changeAuthConfiguration(AuthConfiguration authConfiguration, RestAuthConfig restAuthConfig) {
        return authConfiguration.change(authChange -> {
            authChange.changeProviders(providers -> {
                restAuthConfig.providers().forEach(provider -> {
                    providers.create(provider.name(), cfg -> applyProviderChange(cfg, provider));
                });
            });
            authChange.changeEnabled(restAuthConfig.enabled());
        });
    }

    private static void applyProviderChange(AuthProviderChange change, AuthProviderConfig provider) {
        if (provider.type() == AuthType.BASIC) {
            BasicAuthProviderConfig basicAuthProvider = (BasicAuthProviderConfig) provider;
            change.convert(BasicAuthProviderChange.class)
                    .changeLogin(basicAuthProvider.login())
                    .changePassword(basicAuthProvider.password())
                    .changeName(provider.name());
        } else {
            LOG.error("Couldn't make configuration change for type: ", provider.type());
        }
    }
}
