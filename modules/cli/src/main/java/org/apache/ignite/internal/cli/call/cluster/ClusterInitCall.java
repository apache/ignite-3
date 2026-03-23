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

package org.apache.ignite.internal.cli.call.cluster;

import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.apache.ignite.internal.cli.core.call.DefaultCallOutput.failure;
import static org.apache.ignite.internal.cli.core.call.DefaultCallOutput.success;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cli.core.call.AsyncCall;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.call.ProgressTracker;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.rest.client.api.ClusterManagementApi;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.InitCommand;

/**
 * Inits cluster.
 */
public class ClusterInitCall implements AsyncCall<ClusterInitCallInput, String> {
    private final ProgressTracker tracker;

    private final ApiClientFactory clientFactory;

    /**
     * Custom read timeout for the init operation, default is 10 seconds which could be not enough.
     */
    private static final int READ_TIMEOUT_MILLIS = 60_000;

    ClusterInitCall(ProgressTracker tracker, ApiClientFactory clientFactory) {
        this.tracker = tracker;
        this.clientFactory = clientFactory;
    }

    @Override
    public CompletableFuture<CallOutput<String>> execute(ClusterInitCallInput input) {
        ClusterManagementApi client = createApiClient(input);

        tracker.maxSize(-1);
        return supplyAsync(() -> {
            try {
                client.init(new InitCommand()
                        .metaStorageNodes(input.getMetaStorageNodes())
                        .cmgNodes(input.getCmgNodes())
                        .clusterName(input.getClusterName())
                        .clusterConfiguration(input.clusterConfiguration())
                );
                return success("Cluster was initialized successfully.");
            } catch (ApiException | IllegalArgumentException e) {
                return failure(new IgniteCliApiException(e, input.getClusterUrl()));
            } finally {
                tracker.done();
            }
        });
    }

    private ClusterManagementApi createApiClient(ClusterInitCallInput input) {
        ApiClient client = clientFactory.getClient(input.getClusterUrl());
        client.setReadTimeout(READ_TIMEOUT_MILLIS);
        return new ClusterManagementApi(client);
    }
}
