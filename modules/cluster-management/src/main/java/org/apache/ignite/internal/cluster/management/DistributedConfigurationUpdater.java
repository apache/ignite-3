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
import org.apache.ignite.internal.cluster.management.network.auth.Authentication;
import org.apache.ignite.internal.cluster.management.network.auth.AuthenticationProvider;
import org.apache.ignite.internal.cluster.management.network.auth.BasicAuthenticationProvider;
import org.apache.ignite.internal.configuration.AuthenticationConfiguration;
import org.apache.ignite.internal.configuration.AuthenticationProviderChange;
import org.apache.ignite.internal.configuration.BasicAuthenticationProviderChange;
import org.apache.ignite.internal.configuration.SecurityConfiguration;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.security.AuthenticationType;

/**
 * Updater is responsible for applying changes to the cluster configuration when it's ready.
 */
public class DistributedConfigurationUpdater implements IgniteComponent {

    private static final IgniteLogger LOG = Loggers.forClass(DistributedConfigurationUpdater.class);

    private final CompletableFuture<SecurityConfiguration> securityConfigurationFuture = new CompletableFuture<>();

    public void setClusterRestConfiguration(SecurityConfiguration securityConfiguration) {
        securityConfigurationFuture.complete(securityConfiguration);
    }

    /**
     * Applies changes to the {@link AuthenticationConfiguration} when {@link DistributedConfigurationUpdater#securityConfigurationFuture}
     * is complete.
     *
     * @param authentication {@link AuthenticationConfiguration} that should be applied.
     * @return Future that will be completed when {@link AuthenticationConfiguration} is applied.
     */
    public CompletableFuture<Void> updateRestAuthConfiguration(Authentication authentication) {
        return securityConfigurationFuture.thenApply(SecurityConfiguration::authentication)
                .thenCompose(configuration -> changeAuthConfiguration(configuration, authentication))
                .whenComplete((v, e) -> {
                    if (e != null) {
                        LOG.error("Unable to change auth configuration", e);
                    } else {
                        LOG.info("REST configuration updated successfully");
                    }
                });
    }

    private static CompletableFuture<Void> changeAuthConfiguration(AuthenticationConfiguration authConfiguration,
            Authentication authentication) {
        return authConfiguration.change(authChange -> {
            authChange.changeProviders(providers -> {
                authentication.providers().forEach(provider -> {
                    providers.create(provider.name(), cfg -> applyProviderChange(cfg, provider));
                });
            });
            authChange.changeEnabled(authentication.enabled());
        });
    }

    private static void applyProviderChange(AuthenticationProviderChange change, AuthenticationProvider provider) {
        AuthenticationType type = AuthenticationType.parse(provider.type());
        if (type == AuthenticationType.BASIC) {
            BasicAuthenticationProvider basicAuthProvider = (BasicAuthenticationProvider) provider;
            change.convert(BasicAuthenticationProviderChange.class)
                    .changeLogin(basicAuthProvider.login())
                    .changePassword(basicAuthProvider.password())
                    .changeName(provider.name());
        } else {
            throw new IllegalArgumentException("Unexpected authentication type: " + type);
        }
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() throws Exception {
        securityConfigurationFuture.completeExceptionally(new NodeStoppingException("Component is stopped."));
    }
}
