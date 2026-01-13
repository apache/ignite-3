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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
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
            return completedFuture(DefaultCallOutput.failure(new FileNotFoundException(path.toString())));
        }

        DeploymentContent content;
        try {
            content = input.recursive() ? createZipContent(path) : createFilesContent(path);
        } catch (IOException e) {
            return completedFuture(DefaultCallOutput.failure(e));
        }

        return executeDeploy(input, api, content);
    }

    private static DeploymentContent createFilesContent(Path path) throws IOException {
        List<File> files;
        try (Stream<Path> stream = Files.walk(path, 1)) {
            files = stream
                    .filter(Files::isRegularFile)
                    .map(Path::toFile)
                    .collect(Collectors.toList());
        }
        return new FilesDeploymentContent(files);
    }

    private static DeploymentContent createZipContent(Path path) throws IOException {
        File zipFile = createZipFromDirectory(path);
        return new ZipDeploymentContent(zipFile);
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
                return DefaultCallOutput.failure(e);
            } finally {
                content.cleanup();
            }
            if (call.isCanceled()) {
                return DefaultCallOutput.failure(new RuntimeException("Unit deployment process was canceled"));
            } else if (callback.exception() != null) {
                return handleException(callback.exception(), input);
            } else {
                return DefaultCallOutput.success(MessageUiComponent.from(UiElements.done()).render());
            }
        });
    }

    private static File createZipFromDirectory(Path sourceDir) throws IOException {
        Path zipPath = Files.createTempFile("deploy-unit-", ".zip");
        try (OutputStream os = Files.newOutputStream(zipPath);
                ZipOutputStream zos = new ZipOutputStream(os)) {
            try (Stream<Path> stream = Files.walk(sourceDir)) {
                stream.filter(Files::isRegularFile).forEach(filePath -> {
                    Path relativePath = sourceDir.relativize(filePath);
                    try {
                        ZipEntry zipEntry = new ZipEntry(relativePath.toString().replace('\\', '/'));
                        zos.putNextEntry(zipEntry);
                        Files.copy(filePath, zos);
                        zos.closeEntry();
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to add file to ZIP: " + filePath, e);
                    }
                });
            }
        }
        return zipPath.toFile();
    }

    /** Abstraction for deployment content that can be either files or a ZIP archive. */
    private interface DeploymentContent {
        /** Executes the deployment and returns the HTTP call. */
        Call deploy(
                DeployUnitClient api,
                String unitId,
                String version,
                @Nullable DeployMode deployMode,
                @Nullable List<String> initialNodes,
                TrackingCallback<Boolean> callback
        );

        /** Performs cleanup after deployment (e.g., deletes temporary files). */
        void cleanup();
    }

    /** Deployment content for regular files (non-recursive). */
    private static class FilesDeploymentContent implements DeploymentContent {
        private final List<File> files;

        FilesDeploymentContent(List<File> files) {
            this.files = files;
        }

        @Override
        public Call deploy(
                DeployUnitClient api,
                String unitId,
                String version,
                @Nullable DeployMode deployMode,
                @Nullable List<String> initialNodes,
                TrackingCallback<Boolean> callback
        ) {
            return api.deployUnitAsync(unitId, files, version, deployMode, initialNodes, callback);
        }

        @Override
        public void cleanup() {
            // No cleanup needed for regular files
        }
    }

    /** Deployment content for ZIP archive (recursive). */
    private static class ZipDeploymentContent implements DeploymentContent {
        private final File zipFile;

        ZipDeploymentContent(File zipFile) {
            this.zipFile = zipFile;
        }

        @Override
        public Call deploy(
                DeployUnitClient api,
                String unitId,
                String version,
                @Nullable DeployMode deployMode,
                @Nullable List<String> initialNodes,
                TrackingCallback<Boolean> callback
        ) {
            return api.deployZipUnitAsync(unitId, zipFile, version, deployMode, initialNodes, callback);
        }

        @Override
        public void cleanup() {
            zipFile.delete();
        }
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
