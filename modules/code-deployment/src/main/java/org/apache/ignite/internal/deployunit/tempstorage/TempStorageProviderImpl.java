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

package org.apache.ignite.internal.deployunit.tempstorage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executor;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitWriteException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Implementation of {@link TempStorageProvider} that creates temporary storage directories on the local file system.
 */
public class TempStorageProviderImpl implements TempStorageProvider {
    private static final IgniteLogger LOG = Loggers.forClass(TempStorageProviderImpl.class);

    private final Executor executor;

    private Path storageDir;

    private boolean caseInsensitiveFileSystem;

    public TempStorageProviderImpl(Executor executor) {
        this.executor = executor;
    }

    public void init(Path storageDir) {
        this.storageDir = storageDir;
        this.caseInsensitiveFileSystem = detectCaseInsensitivity(storageDir);
    }

    private static boolean detectCaseInsensitivity(Path storageDir) {
        try {
            Files.createDirectories(storageDir);
            Path probeFile = storageDir.resolve("CaseSensitivityProbe");
            Files.createFile(probeFile);
            try {
                return Files.exists(storageDir.resolve("casesensitivityprobe"));
            } finally {
                Files.deleteIfExists(probeFile);
            }
        } catch (IOException e) {
            LOG.warn("Failed to detect file system case sensitivity, assuming case-insensitive", e);
            return true;
        }
    }

    @Override
    public TempStorage tempStorage(String id, Version version) {
        try {
            Path storageDir = this.storageDir.resolve(id).resolve(version.render());
            Files.createDirectories(storageDir);
            return new TempStorageImpl(storageDir, executor, caseInsensitiveFileSystem);
        } catch (IOException ex) {
            LOG.error("Failed to create temp storage {} with id {} and version {}", ex, storageDir, id, version);
            throw new DeploymentUnitWriteException("Failed to create deployment unit temp storage.", ex);
        }
    }
}
