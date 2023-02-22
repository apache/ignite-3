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

import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.rest.client.invoker.ApiClient;

/**
 * This class holds the map from URLs to {@link ApiClient}.
 */
@Singleton
public class ApiClientFactory {
    private final Map<String, ApiClient> clients = new HashMap<>();

    /**
     * Gets {@link ApiClient} for the base path.
     *
     * @param path Base path.
     * @return created API client.
     */
    public ApiClient getClient(String path) {
        ApiClient apiClient = clients.get(path);
        if (apiClient == null) {
            apiClient = new ApiClient().setBasePath(path);
            clients.put(path, apiClient);
            CliLoggers.addApiClient(apiClient);
        }
        return apiClient;
    }
}
