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

package org.apache.ignite.internal.cli.call.cluster.unit;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.cli.core.call.DefaultCallOutput.failure;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import okhttp3.Call;
import org.apache.ignite.internal.cli.core.call.AsyncCall;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.call.ProgressTracker;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.exception.UnitAlreadyExistsException;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.internal.cli.core.style.component.MessageUiComponent;
import org.apache.ignite.internal.cli.core.style.element.UiElements;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.model.DeployMode;
import org.jetbrains.annotations.Nullable;

/** Call to deploy a unit. */
public class DeployUnitCall implements AsyncCall<DeployUnitCallInput, String> {

    private final ProgressTracker tracker;

    private final ApiClientFactory clientFactory;

    DeployUnitCall(ProgressTracker tracker, ApiClientFactory clientFactory) {
        this.tracker = tracker;
        this.clientFactory = clientFactory;
    }

    @Override
    public CompletableFuture<CallOutput<String>> execute(DeployUnitCallInput input) {
        ApiClient apiClient = clientFactory.getClient(input.clusterUrl());
        DeployUnitClient api = new DeployUnitClient(apiClient);

        Path path = input.path();
        if (Files.notExists(path)) {
            return completedFuture(failure(new FileNotFoundException(path.toString())));
        }

        try {
            DeploymentContent content = input.recursive()
                    ? ZipDeploymentContent.fromDirectory(path)
                    : FilesDeploymentContent.fromPath(path);
            return executeDeploy(input, api, content);
        } catch (IOException e) {
            return completedFuture(failure(e));
        }
    }

    private CompletableFuture<CallOutput<String>> executeDeploy(
            DeployUnitCallInput input,
            DeployUnitClient api,
            DeploymentContent content
    ) {
        TrackingCallback<Boolean> callback = new TrackingCallback<>(tracker);
        String ver = input.version() == null ? "" : input.version();
        DeployMode deployMode = inferDeployMode(input.nodes());
        List<String> initialNodes = deployMode == null ? input.nodes() : null;

        Call call = content.deploy(api, input.id(), ver, deployMode, initialNodes, callback);

        return CompletableFuture.supplyAsync(() -> {
            try {
                callback.awaitDone();
            } catch (InterruptedException e) {
                return failure(e);
            } finally {
                content.cleanup();
            }
            if (call.isCanceled()) {
                return failure(new RuntimeException("Unit deployment process was canceled"));
            } else if (callback.exception() != null) {
                return handleException(callback.exception(), input);
            } else {
                return DefaultCallOutput.success(MessageUiComponent.from(UiElements.done()).render());
            }
        });
    }

    private static CallOutput<String> handleException(Exception exception, DeployUnitCallInput input) {
        if (exception instanceof ApiException) {
            ApiException apiException = (ApiException) exception;
            if (apiException.getCode() == 409) {
                // special case when cluster is not initialized
                if (apiException.getResponseBody().contains("Cluster is not initialized")) {
                    return failure(new IgniteCliApiException(exception, input.clusterUrl()));
                }
                return failure(new UnitAlreadyExistsException(input.id(), input.version()));
            }
        }

        return failure(new IgniteCliApiException(exception, input.clusterUrl()));
    }

    @Nullable
    private static DeployMode inferDeployMode(List<String> params) {
        if (params == null) {
            return DeployMode.MAJORITY;
        }
        if (params.size() == 1) {
            try {
                return DeployMode.fromValue(params.get(0));
            } catch (IllegalArgumentException ignored) {
                return null;
            }
        }
        return null;
    }
}
