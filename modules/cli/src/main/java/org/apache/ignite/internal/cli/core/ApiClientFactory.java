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

import static org.apache.ignite.internal.cli.config.CliConfigKeys.KEY_STORE_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.KEY_STORE_PATH;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.TRUST_STORE_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.TRUST_STORE_PATH;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.cli.config.ConfigManager;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.core.exception.IgniteCliException;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.rest.client.invoker.ApiClient;

/**
 * This class holds the map from URLs to {@link ApiClient}.
 */
@Singleton
public class ApiClientFactory {

    @Inject
    private final ConfigManagerProvider configManagerProvider;

    private final Map<String, ApiClient> clients = new ConcurrentHashMap<>();

    public ApiClientFactory(ConfigManagerProvider configManagerProvider) {
        this.configManagerProvider = configManagerProvider;
    }

    /**
     * Gets {@link ApiClient} for the base path.
     *
     * @param path Base path.
     * @return created API client.
     */
    public ApiClient getClient(String path) {
        ApiClient apiClient = clients.computeIfAbsent(path, this::createClient);
        CliLoggers.addApiClient(path, apiClient);
        return apiClient;
    }

    private ApiClient createClient(String path) {
        try {
            ConfigManager configManager = configManagerProvider.get();
            return ApiClientBuilder.create()
                    .basePath(path)
                    .keyStorePath(configManager.getCurrentProperty(KEY_STORE_PATH.value()))
                    .keyStorePassword(configManager.getCurrentProperty(KEY_STORE_PASSWORD.value()))
                    .trustStorePath(configManager.getCurrentProperty(TRUST_STORE_PATH.value()))
                    .trustStorePassword(configManager.getCurrentProperty(TRUST_STORE_PASSWORD.value()))
                    .build();
        } catch (Exception e) {
            throw new IgniteCliException("Couldn't build REST client", e);
        }
    }
}
