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

import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import okhttp3.Call;
import org.apache.ignite.rest.client.model.DeployMode;
import org.jetbrains.annotations.Nullable;

/** Deployment content for ZIP archive (recursive directory deployment). */
class ZipDeploymentContent implements DeploymentContent {
    private final File zipFile;

    private ZipDeploymentContent(File zipFile) {
        this.zipFile = zipFile;
    }

    /**
     * Creates a deployment content by zipping the given directory recursively.
     *
     * @param sourceDir The directory to zip.
     * @return The deployment content.
     * @throws IOException If an I/O error occurs.
     */
    static ZipDeploymentContent fromDirectory(Path sourceDir) throws IOException {
        File zipFile = createZipFromDirectory(sourceDir);
        return new ZipDeploymentContent(zipFile);
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
                        throw sneakyThrow(e);
                    }
                });
            }
        }
        return zipPath.toFile();
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
