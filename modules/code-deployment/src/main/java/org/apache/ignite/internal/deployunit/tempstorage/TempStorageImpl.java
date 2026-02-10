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

import static java.util.concurrent.CompletableFuture.supplyAsync;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitWriteException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Implementation of {@link TempStorage} that stores files in a local file system directory.
 */
class TempStorageImpl implements TempStorage {
    private static final IgniteLogger LOG = Loggers.forClass(TempStorageImpl.class);

    private final Path storageDir;

    private final Map<String, CompletableFuture<Path>> files = new HashMap<>();

    private final Executor executor;

    private final boolean caseInsensitiveFileSystem;

    TempStorageImpl(Path storageDir, Executor executor, boolean caseInsensitiveFileSystem) {
        this.storageDir = storageDir;
        this.executor = executor;
        this.caseInsensitiveFileSystem = caseInsensitiveFileSystem;
    }

    @Override
    public CompletableFuture<Path> store(String fileName, InputStream is) {
        CompletableFuture<Path> result = supplyAsync(() -> {
            try {
                Path path = Path.of(fileName);
                Path parent = path.getParent();
                if (parent != null) {
                    Files.createDirectories(storageDir.resolve(parent));
                }
                Path resolve = storageDir.resolve(path);
                Files.copy(is, resolve, StandardCopyOption.REPLACE_EXISTING);
                return resolve;
            } catch (Exception e) {
                LOG.error("Failed to process unit storage action.", e);
                throw new DeploymentUnitWriteException("Failed to write unit to storage.", e);
            }
        }, executor);

        files.put(fileName, result);
        return result;
    }

    @Override
    public void rollback() {
        files.values().forEach(f -> f.cancel(true));
    }

    @Override
    public void close() {
        IgniteUtils.deleteIfExists(storageDir);
    }

    @Override
    public boolean isCaseInsensitiveFileSystem() {
        return caseInsensitiveFileSystem;
    }
}
