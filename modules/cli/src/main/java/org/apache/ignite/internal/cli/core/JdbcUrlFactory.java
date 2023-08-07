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

package org.apache.ignite.internal.cli.core;

import static org.apache.ignite.internal.cli.config.CliConfigKeys.BASIC_AUTHENTICATION_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.BASIC_AUTHENTICATION_USERNAME;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.JDBC_CLIENT_AUTH;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.JDBC_KEY_STORE_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.JDBC_KEY_STORE_PATH;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.JDBC_SSL_ENABLED;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.JDBC_TRUST_STORE_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.JDBC_TRUST_STORE_PATH;

import com.google.gson.Gson;
import jakarta.inject.Singleton;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.ConfigManager;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.core.repl.config.RootConfig;
import org.apache.ignite.lang.util.StringUtils;
import org.jetbrains.annotations.Nullable;

/** Ignite JDBC URL factory. */
@Singleton
public class JdbcUrlFactory {
    private final ConfigManagerProvider configManagerProvider;

    /** Constructor. */
    public JdbcUrlFactory(ConfigManagerProvider configManagerProvider) {
        this.configManagerProvider = configManagerProvider;
    }

    /**
     * Constructs JDBC URL from node URL, port taken from the node configuration, SSL and basic authentication properties from the config.
     *
     * @param configuration Node configuration in HOCON format.
     * @param nodeUrl Node URL.
     * @return JDBC URL.
     */
    @Nullable
    public String constructJdbcUrl(String configuration, String nodeUrl) {
        try {
            int port = new Gson().fromJson(configuration, RootConfig.class).clientConnector.port;
            String host = new URL(nodeUrl).getHost();
            return applyConfig("jdbc:ignite:thin://" + host + ":" + port);
        } catch (MalformedURLException ignored) {
            return null;
        }
    }

    private String applyConfig(String jdbcUrl) {
        List<String> queryParams = new ArrayList<>();
        addIfSet(queryParams, JDBC_TRUST_STORE_PATH, "trustStorePath");
        addIfSet(queryParams, JDBC_TRUST_STORE_PASSWORD, "trustStorePassword");
        addIfSet(queryParams, JDBC_KEY_STORE_PATH, "keyStorePath");
        addIfSet(queryParams, JDBC_KEY_STORE_PASSWORD, "keyStorePassword");
        addIfSet(queryParams, JDBC_CLIENT_AUTH, "clientAuth");
        addSslEnabledIfNeeded(queryParams);
        addIfSet(queryParams, BASIC_AUTHENTICATION_USERNAME, "basicAuthenticationUsername");
        addIfSet(queryParams, BASIC_AUTHENTICATION_PASSWORD, "basicAuthenticationPassword");
        if (!queryParams.isEmpty()) {
            String query = queryParams.stream()
                    .collect(Collectors.joining("&", "?", ""));
            return jdbcUrl + query;
        } else {
            return jdbcUrl;
        }
    }

    private void addSslEnabledIfNeeded(List<String> queryParams) {
        String sslEnabled = configManagerProvider.get().getCurrentProperty(JDBC_SSL_ENABLED.value());
        if (sslEnabled != null) {
            queryParams.add(0, "sslEnabled=" + sslEnabled);
        } else if (!queryParams.isEmpty()) {
            queryParams.add(0, "sslEnabled=true");
        }
    }

    private void addIfSet(List<String> queryParams, CliConfigKeys key, String property) {
        ConfigManager configManager = configManagerProvider.get();
        String value = configManager.getCurrentProperty(key.value());
        if (!StringUtils.nullOrBlank(value)) {
            queryParams.add(property + "=" + value);
        }
    }
}
