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

package org.apache.ignite.internal.rest.cluster;

import static org.apache.ignite.security.AuthenticationConfig.disabled;

import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.ClusterInitializer;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.rest.api.cluster.ClusterManagementApi;
import org.apache.ignite.internal.rest.api.cluster.ClusterState;
import org.apache.ignite.internal.rest.api.cluster.ClusterTag;
import org.apache.ignite.internal.rest.api.cluster.InitCommand;
import org.apache.ignite.internal.rest.api.cluster.authentication.AuthenticationConfig;
import org.apache.ignite.internal.rest.api.cluster.authentication.AuthenticationProviderConfig;
import org.apache.ignite.internal.rest.api.cluster.authentication.BasicAuthenticationProviderConfig;
import org.apache.ignite.internal.rest.cluster.exception.InvalidArgumentClusterInitializationException;
import org.apache.ignite.internal.rest.exception.ClusterNotInitializedException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.security.AuthenticationType;

/**
 * Cluster management controller implementation.
 */
@Controller("/management/v1/cluster")
public class ClusterManagementController implements ClusterManagementApi {
    private static final IgniteLogger LOG = Loggers.forClass(ClusterManagementController.class);

    private final ClusterInitializer clusterInitializer;

    private final ClusterManagementGroupManager clusterManagementGroupManager;

    /**
     * Cluster management controller constructor.
     *
     * @param clusterInitializer cluster initializer.
     * @param clusterManagementGroupManager cluster management group manager.
     */
    public ClusterManagementController(ClusterInitializer clusterInitializer, ClusterManagementGroupManager clusterManagementGroupManager) {
        this.clusterInitializer = clusterInitializer;
        this.clusterManagementGroupManager = clusterManagementGroupManager;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<ClusterState> clusterState() {
        return clusterManagementGroupManager.clusterState().thenApply(this::mapClusterState);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> init(@Body InitCommand initCommand) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Received init command [metaStorageNodes={}, cmgNodes={}]", initCommand.metaStorageNodes(),
                    initCommand.cmgNodes());
        }

        AuthenticationConfig authenticationConfigDto = initCommand.authenticationConfig();
        org.apache.ignite.security.AuthenticationConfig authenticationConfig = authenticationConfigDto == null
                ? disabled()
                : authnConfigDtoToRestAuthnConfig(authenticationConfigDto);

        return clusterInitializer.initCluster(
                        initCommand.metaStorageNodes(),
                        initCommand.cmgNodes(),
                        initCommand.clusterName(),
                        authenticationConfig
                )
                .exceptionally(ex -> {
                    throw mapException(ex);
                });
    }

    private ClusterState mapClusterState(org.apache.ignite.internal.cluster.management.ClusterState clusterState) {
        if (clusterState == null) {
            throw new ClusterNotInitializedException();
        }

        return new ClusterState(
                clusterState.cmgNodes(),
                clusterState.metaStorageNodes(),
                clusterState.igniteVersion().toString(),
                new ClusterTag(clusterState.clusterTag().clusterName(), clusterState.clusterTag().clusterId())
        );
    }

    private RuntimeException mapException(Throwable ex) {
        if (ex instanceof CompletionException) {
            var cause = ex.getCause();
            if (cause instanceof IgniteInternalException) {
                return (IgniteInternalException) cause;
            }
        }

        if (ex instanceof IllegalArgumentException) {
            return new InvalidArgumentClusterInitializationException(ex);
        }

        return new IgniteException(ex);
    }

    private org.apache.ignite.security.AuthenticationConfig authnConfigDtoToRestAuthnConfig(AuthenticationConfig configDto) {
        return new org.apache.ignite.security.AuthenticationConfig(configDto.enabled(), authnProviders(configDto.providers()));
    }

    private List<org.apache.ignite.security.AuthenticationProviderConfig> authnProviders(List<AuthenticationProviderConfig> providers) {
        if (providers == null) {
            return Collections.emptyList();
        } else {
            return providers.stream()
                    .map(this::authnProviderConfigDtoToAuthnProviderConfig)
                    .collect(Collectors.toList());
        }
    }

    private org.apache.ignite.security.AuthenticationProviderConfig authnProviderConfigDtoToAuthnProviderConfig(
            AuthenticationProviderConfig configDto) {
        AuthenticationType type = configDto.type();
        if (type == AuthenticationType.BASIC) {
            BasicAuthenticationProviderConfig basicAuthenticationProviderConfigDto = (BasicAuthenticationProviderConfig) configDto;
            return new org.apache.ignite.security.BasicAuthenticationProviderConfig(
                    basicAuthenticationProviderConfigDto.name(),
                    basicAuthenticationProviderConfigDto.username(),
                    basicAuthenticationProviderConfigDto.password()
            );
        } else {
            throw new IllegalArgumentException("Unexpected auth type: " + type);
        }
    }
}
