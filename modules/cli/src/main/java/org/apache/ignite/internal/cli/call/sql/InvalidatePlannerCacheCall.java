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

package org.apache.ignite.internal.cli.call.sql;

import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.rest.client.api.SqlApi;
import org.apache.ignite.rest.client.invoker.ApiException;

/**
 * Shows node configuration from ignite cluster.
 */
@Singleton
public class InvalidatePlannerCacheCall implements Call<InvalidateCacheCallInput, String> {
    private final ApiClientFactory clientFactory;

    public InvalidatePlannerCacheCall(ApiClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    /** {@inheritDoc} */
    @Override
    public DefaultCallOutput<String> execute(InvalidateCacheCallInput input) {
        SqlApi client = createApiClient(input);

        try {
            client.clearCache(input.getTables());
            return DefaultCallOutput.success("Successfully cleared SQL query plan cache.");
        } catch (ApiException e) {
            return DefaultCallOutput.failure(new IgniteCliApiException(e, input.clusterUrl()));
        }
    }

    private SqlApi createApiClient(InvalidateCacheCallInput input) {
        return new SqlApi(clientFactory.getClient(input.clusterUrl()));
    }
}
