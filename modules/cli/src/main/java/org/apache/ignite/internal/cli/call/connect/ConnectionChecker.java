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

package org.apache.ignite.internal.cli.call.connect;

import static org.apache.ignite.internal.cli.config.CliConfigKeys.BASIC_AUTHENTICATION_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.BASIC_AUTHENTICATION_USERNAME;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.REST_KEY_STORE_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.REST_KEY_STORE_PATH;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.REST_TRUST_STORE_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.REST_TRUST_STORE_PATH;
import static org.apache.ignite.lang.util.StringUtils.nullOrBlank;

import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.ConfigManager;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.core.JdbcUrlFactory;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.internal.cli.core.rest.ApiClientSettings;
import org.apache.ignite.internal.cli.core.rest.ApiClientSettingsBuilder;
import org.apache.ignite.lang.util.StringUtils;
import org.apache.ignite.rest.client.api.NodeConfigurationApi;
import org.apache.ignite.rest.client.api.NodeManagementApi;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;

/**
 * Checks connection to the Ignite3 node. Creates {@link SessionInfo} on success.
 */
@Singleton
public class ConnectionChecker {

    private final JdbcUrlFactory jdbcUrlFactory;

    private final ConfigManagerProvider configManagerProvider;

    public ConnectionChecker(JdbcUrlFactory jdbcUrlFactory, ConfigManagerProvider configManagerProvider) {
        this.jdbcUrlFactory = jdbcUrlFactory;
        this.configManagerProvider = configManagerProvider;
    }

    /**
     * Check connection to the node. Creates {@link SessionInfo} on success.
     *
     * @param callInput input parameters
     * @param sslConfig ssl config
     * @return session info on successful connection.
     * @throws ApiException if connection can't be established.
     */
    public SessionInfo checkConnection(ConnectCallInput callInput, SslConfig sslConfig) throws ApiException {
        ApiClientSettingsBuilder settingsBuilder = ApiClientSettings.builder()
                .basePath(callInput.url());

        if (sslConfig != null) {
            settingsBuilder.keyStorePath(sslConfig.keyStorePath())
                    .keyStorePassword(sslConfig.keyStorePassword())
                    .trustStorePath(sslConfig.trustStorePath())
                    .trustStorePassword(sslConfig.trustStorePassword());
        } else {
            ConfigManager configManager = configManagerProvider.get();
            settingsBuilder.keyStorePath(configManager.getCurrentProperty(REST_KEY_STORE_PATH.value()))
                    .keyStorePassword(configManager.getCurrentProperty(REST_KEY_STORE_PASSWORD.value()))
                    .trustStorePath(configManager.getCurrentProperty(REST_TRUST_STORE_PATH.value()))
                    .trustStorePassword(configManager.getCurrentProperty(REST_TRUST_STORE_PASSWORD.value()));
        }

        if (!nullOrBlank(callInput.username()) && !nullOrBlank(callInput.password())) {
            settingsBuilder.basicAuthenticationUsername(callInput.username());
            settingsBuilder.basicAuthenticationPassword(callInput.password());

        }
        ApiClient apiClient = ApiClientFactory.buildClient(settingsBuilder.build());

        String configuration = new NodeConfigurationApi(apiClient).getNodeConfiguration();
        String nodeName = new NodeManagementApi(apiClient).nodeState().getName();
        String jdbcUrl = jdbcUrlFactory.constructJdbcUrl(configuration, callInput.url());
        return SessionInfo.builder().nodeUrl(callInput.url()).nodeName(nodeName).jdbcUrl(jdbcUrl).username(callInput.username()).build();
    }

    /**
     * Save settings in cli config.
     *
     * @param callInput input parameters
     * @param sslConfig ssl config
     */
    public void saveSettings(ConnectCallInput callInput, SslConfig sslConfig) {
        ConfigManager manager = configManagerProvider.get();
        if (sslConfig != null) {
            manager.setProperty(CliConfigKeys.REST_TRUST_STORE_PATH.value(), sslConfig.trustStorePath());
            manager.setProperty(CliConfigKeys.REST_TRUST_STORE_PASSWORD.value(), sslConfig.trustStorePassword());
            if (!StringUtils.nullOrBlank(sslConfig.keyStorePath())) {
                manager.setProperty(CliConfigKeys.REST_KEY_STORE_PATH.value(), sslConfig.keyStorePath());
                manager.setProperty(CliConfigKeys.REST_KEY_STORE_PASSWORD.value(), sslConfig.keyStorePassword());
            }
        }

        if (!nullOrBlank(callInput.username()) && !nullOrBlank(callInput.password())) {
            manager.setProperty(BASIC_AUTHENTICATION_USERNAME.value(), callInput.username());
            manager.setProperty(BASIC_AUTHENTICATION_PASSWORD.value(), callInput.password());
        }
    }
}
