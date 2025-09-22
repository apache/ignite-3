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

package org.apache.ignite.internal.deployunit;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map.Entry;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Implementation of {@link DeploymentUnitProcessor} that deploys deployment unit content to the file system.
 *
 * <p>This processor extracts and deploys files from deployment units to a specified target directory.
 * It handles both regular deployment units (containing individual files as input streams) and ZIP-based
 * deployment units (containing compressed archives that need extraction).
 *
 * <p>The deployment process ensures atomic file operations by:
 * <ul>
 *     <li>First copying files to temporary locations with a {@code .tmp} suffix</li>
 *     <li>Then atomically moving them to their final destinations</li>
 *     <li>Creating necessary parent directories as needed</li>
 * </ul>
 *
 * <p>This approach prevents partial deployments and ensures that files are either fully deployed
 * or not deployed at all, maintaining consistency during the deployment process.
 *
 * <p>Type parameters:
 * <ul>
 *     <li>{@code Path} - the argument type representing the target deployment directory</li>
 *     <li>{@code Void} - the return type (no meaningful return value)</li>
 * </ul>
 */
public class DeployDeploymentUnitProcessor implements DeploymentUnitProcessor<Path> {
    /** Suffix used for temporary files during the deployment process. */
    private static final String TMP_SUFFIX = ".tmp";

    @Override
    public void processContent(DeploymentUnitImpl unit, Path unitFolder) throws IOException {
        for (Entry<String, InputStream> e : unit.content().entrySet()) {
            doDeploy(unitFolder, e.getKey(), e.getValue());
        }
    }

    @Override
    public void processContentWithUnzip(ZipDeploymentUnit unit, Path unitFolder) throws IOException {
        for (Entry<String, ZipInputStream> e : unit.zipContent().entrySet()) {
            ZipInputStream zis = e.getValue();
            String name = e.getKey();
            Path zipUnitFolder = unitFolder.resolve(name);
            ZipEntry ze;
            while ((ze = zis.getNextEntry()) != null) {
                Path entryPath = zipUnitFolder.resolve(ze.getName());

                if (ze.isDirectory()) {
                    Files.createDirectories(entryPath);
                } else {
                    Path unitFileFolder = entryPath.getParent();
                    Files.createDirectories(unitFileFolder);
                    doDeploy(unitFileFolder, entryPath.getFileName().toString(), zis);
                }
            }
        }
    }

    private static void doDeploy(Path unitFolder, String entryName, InputStream is) throws IOException {
        Path unitPath = unitFolder.resolve(entryName);
        Files.createDirectories(unitPath.getParent());
        Path unitPathTmp = unitFolder.resolve(entryName + TMP_SUFFIX);
        Files.copy(is, unitPathTmp, REPLACE_EXISTING);
        Files.move(unitPathTmp, unitPath, ATOMIC_MOVE, REPLACE_EXISTING);
    }
}
