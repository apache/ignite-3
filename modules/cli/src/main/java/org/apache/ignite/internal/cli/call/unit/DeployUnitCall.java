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

package org.apache.ignite.internal.cli.call.unit;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import okhttp3.Call;
import org.apache.ignite.internal.cli.core.call.AsyncCall;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.call.ProgressTracker;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.core.exception.UnitAlreadyExistsException;
import org.apache.ignite.internal.cli.core.repl.registry.UnitsRegistry;
import org.apache.ignite.internal.cli.core.rest.ApiClientFactory;
import org.apache.ignite.internal.cli.core.style.component.MessageUiComponent;
import org.apache.ignite.internal.cli.core.style.element.UiElements;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;

/** Call to deploy a unit. */
public class DeployUnitCall implements AsyncCall<DeployUnitCallInput, String> {

    private final ProgressTracker tracker;

    private final ApiClientFactory clientFactory;

    private final UnitsRegistry unitsRegistry;

    DeployUnitCall(ProgressTracker tracker, ApiClientFactory clientFactory, UnitsRegistry registry) {
        this.tracker = tracker;
        this.clientFactory = clientFactory;
        this.unitsRegistry = registry;
    }

    @Override
    public CompletableFuture<CallOutput<String>> execute(DeployUnitCallInput input) {
        ApiClient apiClient = clientFactory.getClient(input.clusterUrl());
        DeployUnitClient api = new DeployUnitClient(apiClient);

        Path path = input.path();
        if (Files.notExists(path)) {
            return completedFuture(DefaultCallOutput.failure(new FileNotFoundException(path.toString())));
        }
        List<File> files;
        if (Files.isDirectory(path)) {
            try (Stream<Path> stream = Files.list(path)) {
                files = stream
                        .filter(Files::isRegularFile)
                        .map(Path::toFile)
                        .collect(Collectors.toList());
            } catch (IOException e) {
                return completedFuture(DefaultCallOutput.failure(e));
            }
        } else {
            files = List.of(path.toFile());
        }

        TrackingCallback<Boolean> callback = new TrackingCallback<>(tracker);
        String ver = input.version() == null ? "" : input.version();
        Call call = api.deployUnitAsync(input.id(), files, ver, callback);

        return CompletableFuture.supplyAsync(() -> {
            try {
                callback.awaitDone();
            } catch (InterruptedException e) {
                return DefaultCallOutput.failure(e);
            }
            if (call.isCanceled()) {
                return DefaultCallOutput.failure(new RuntimeException("Unit deployment process was canceled"));
            } else if (callback.exception() != null) {
                return handleException(callback.exception(), input);
            } else {
                unitsRegistry.refresh();
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
                    return DefaultCallOutput.failure(new IgniteCliApiException(exception, input.clusterUrl()));
                }
                return DefaultCallOutput.failure(new UnitAlreadyExistsException(input.id(), input.version()));
            }
        }

        return DefaultCallOutput.failure(new IgniteCliApiException(exception, input.clusterUrl()));
    }
}
