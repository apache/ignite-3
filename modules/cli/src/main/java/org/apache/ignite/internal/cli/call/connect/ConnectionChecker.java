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
import static org.apache.ignite.internal.cli.config.CliConfigKeys.REST_CIPHERS;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.REST_KEY_STORE_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.REST_KEY_STORE_PATH;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.REST_TRUST_STORE_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.REST_TRUST_STORE_PATH;
import static org.apache.ignite.internal.util.StringUtils.nullOrBlank;

import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.config.ConfigManager;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.core.JdbcUrlFactory;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.internal.cli.core.rest.ApiClientSettings;
import org.apache.ignite.internal.cli.core.rest.ApiClientSettingsBuilder;
import org.apache.ignite.rest.client.api.NodeManagementApi;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.NodeInfo;
import org.jetbrains.annotations.Nullable;

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
     * Check connection to the node. Creates {@link SessionInfo} on success. Use settings from {@link SslConfig}.
     *
     * @param callInput input parameters
     * @param sslConfig ssl config
     * @return session info on successful connection.
     * @throws ApiException if connection can't be established.
     */
    public SessionInfo checkConnection(ConnectCallInput callInput, SslConfig sslConfig) throws ApiException {
        ApiClientSettingsBuilder settingsBuilder = ApiClientSettings.builder()
                .basePath(callInput.url());
        buildSslSettings(sslConfig, settingsBuilder);
        buildAuthSettings(callInput, settingsBuilder);
        return checkConnection(settingsBuilder.build());
    }

    /**
     * Check connection to the node. Creates {@link SessionInfo} on success.
     *
     * @param callInput input parameters
     * @return session info on successful connection.
     * @throws ApiException if connection can't be established.
     */
    public SessionInfo checkConnection(ConnectCallInput callInput) throws ApiException {
        ApiClientSettingsBuilder settingsBuilder = ApiClientSettings.builder()
                .basePath(callInput.url());
        buildSslSettingsFromConfig(settingsBuilder);
        buildAuthSettings(callInput, settingsBuilder);
        return checkConnection(settingsBuilder.build());
    }

    /**
     * Check connection to the node. Creates {@link SessionInfo} on success.
     *
     * @param apiClientSettings input parameters
     * @return session info on successful connection.
     * @throws ApiException if connection can't be established.
     */
    private SessionInfo checkConnection(ApiClientSettings apiClientSettings) throws ApiException {
        ApiClient apiClient = ApiClientFactory.buildClient(apiClientSettings);

        NodeInfo nodeInfo = new NodeManagementApi(apiClient).nodeInfo();
        String jdbcUrl = jdbcUrlFactory.constructJdbcUrl(
                apiClientSettings.basePath(),
                nodeInfo.getJdbcPort(),
                apiClientSettings.basicAuthenticationUsername(),
                apiClientSettings.basicAuthenticationPassword()
        );
        return SessionInfo.builder().nodeUrl(apiClientSettings.basePath())
                .nodeName(nodeInfo.getName()).jdbcUrl(jdbcUrl).username(apiClientSettings.basicAuthenticationUsername()).build();
    }

    /**
     * Check connection to the node without basic authentication. Creates {@link SessionInfo} on success.
     *
     * @param callInput input parameters
     * @return session info on successful connection.
     * @throws ApiException if connection can't be established.
     */
    public SessionInfo checkConnectionWithoutAuthentication(ConnectCallInput callInput) throws ApiException {
        ApiClientSettingsBuilder settingsBuilder = ApiClientSettings.builder()
                .basePath(callInput.url());
        buildSslSettingsFromConfig(settingsBuilder);
        return checkConnection(settingsBuilder.build());
    }

    private void buildAuthSettings(ConnectCallInput callInput, ApiClientSettingsBuilder settingsBuilder) {
        ConfigManager configManager = configManagerProvider.get();

        String username = firstNonNullOrBlankString(callInput.username(),
                configManager.getCurrentProperty(BASIC_AUTHENTICATION_USERNAME.value()));
        String password = firstNonNullOrBlankString(callInput.password(),
                configManager.getCurrentProperty(BASIC_AUTHENTICATION_PASSWORD.value()));

        settingsBuilder.basicAuthenticationUsername(username);
        settingsBuilder.basicAuthenticationPassword(password);
    }

    /**
     * Returns first non-null and non-blank argument. Useful when getting the value from CLI parameter or from the config file.
     *
     * @param first First string.
     * @param second Second string.
     * @return First non-null and non-blank string.
     */
    @Nullable
    public static String firstNonNullOrBlankString(@Nullable String first, @Nullable String second) {
        if (!nullOrBlank(first)) {
            return first;
        }
        if (!nullOrBlank(second)) {
            return second;
        }
        return null;
    }

    private static void buildSslSettings(SslConfig sslConfig, ApiClientSettingsBuilder settingsBuilder) {
        settingsBuilder.keyStorePath(sslConfig.keyStorePath())
                .keyStorePassword(sslConfig.keyStorePassword())
                .trustStorePath(sslConfig.trustStorePath())
                .trustStorePassword(sslConfig.trustStorePassword());
    }

    private void buildSslSettingsFromConfig(ApiClientSettingsBuilder settingsBuilder) {
        ConfigManager configManager = configManagerProvider.get();
        settingsBuilder.keyStorePath(configManager.getCurrentProperty(REST_KEY_STORE_PATH.value()))
                .keyStorePassword(configManager.getCurrentProperty(REST_KEY_STORE_PASSWORD.value()))
                .trustStorePath(configManager.getCurrentProperty(REST_TRUST_STORE_PATH.value()))
                .trustStorePassword(configManager.getCurrentProperty(REST_TRUST_STORE_PASSWORD.value()))
                .ciphers(configManager.getCurrentProperty(REST_CIPHERS.value()));
    }

    /**
     * Save settings in cli config.
     *
     * @param sslConfig ssl config
     */
    public void saveSettings(SslConfig sslConfig) {
        ConfigManager manager = configManagerProvider.get();
        if (sslConfig != null) {
            manager.setProperty(REST_TRUST_STORE_PATH.value(), sslConfig.trustStorePath());
            manager.setProperty(REST_TRUST_STORE_PASSWORD.value(), sslConfig.trustStorePassword());
            if (!nullOrBlank(sslConfig.keyStorePath())) {
                manager.setProperty(REST_KEY_STORE_PATH.value(), sslConfig.keyStorePath());
                manager.setProperty(REST_KEY_STORE_PASSWORD.value(), sslConfig.keyStorePassword());
            }
        }
    }
}
