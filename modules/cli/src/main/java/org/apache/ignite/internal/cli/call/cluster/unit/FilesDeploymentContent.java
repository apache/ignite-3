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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import okhttp3.Call;
import org.apache.ignite.rest.client.model.DeployMode;
import org.jetbrains.annotations.Nullable;

/** Deployment content for regular files (non-recursive). */
class FilesDeploymentContent implements DeploymentContent {
    private final List<File> files;

    private FilesDeploymentContent(List<File> files) {
        this.files = files;
    }

    /**
     * Creates a deployment content from files in the given path (non-recursive, depth 1).
     *
     * @param path The path to collect files from.
     * @return The deployment content.
     * @throws IOException If an I/O error occurs.
     */
    static FilesDeploymentContent fromPath(Path path) throws IOException {
        List<File> files;
        try (Stream<Path> stream = Files.walk(path, 1)) {
            files = stream
                    .filter(Files::isRegularFile)
                    .map(Path::toFile)
                    .collect(Collectors.toList());
        }
        return new FilesDeploymentContent(files);
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
