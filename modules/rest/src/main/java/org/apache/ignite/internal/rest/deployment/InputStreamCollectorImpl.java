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

package org.apache.ignite.internal.rest.deployment;

import static java.util.concurrent.CompletableFuture.allOf;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.deployunit.DeploymentUnit;
import org.apache.ignite.internal.deployunit.FilesDeploymentUnit;
import org.apache.ignite.internal.deployunit.tempstorage.TempStorage;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Standard implementation of {@link InputStreamCollector} for collecting regular file content.
 *
 * <p>This implementation provides a straightforward approach to collecting input streams and
 * converting them into a standard {@link FilesDeploymentUnit}. It maintains an internal map of filename-to-stream associations and creates
 * deployment units containing regular (non-compressed) file content.
 */
public class InputStreamCollectorImpl implements InputStreamCollector {
    private static final IgniteLogger LOG = Loggers.forClass(InputStreamCollectorImpl.class);

    private final Map<String, CompletableFuture<Path>> collect = new HashMap<>();

    private final TempStorage tempStorage;

    public InputStreamCollectorImpl(TempStorage tempStorage) {
        this.tempStorage = tempStorage;
    }

    @Override
    public void addInputStream(String filename, InputStream is) {
        String filenameKey = tempStorage.isCaseInsensitiveFileSystem() ? filename.toLowerCase(Locale.ROOT) : filename;
        if (collect.containsKey(filenameKey)) {
            closeStream(is);
            throw new DuplicateFilenamesException("Duplicate filename: " + filename);
        }

        collect.put(filenameKey, tempStorage.store(filename, is).whenComplete((path, throwable) -> closeStream(is)));
    }

    private static void closeStream(InputStream is) {
        try {
            is.close();
        } catch (IOException e) {
            LOG.error("Error when closing input stream.", e);
        }
    }

    @Override
    public CompletableFuture<DeploymentUnit> toDeploymentUnit() {
        Map<String, Path> map = new ConcurrentHashMap<>();
        for (Entry<String, CompletableFuture<Path>> e : collect.entrySet()) {
            e.getValue().thenAccept(path -> map.put(e.getKey(), path));
        }

        return allOf(collect.values().toArray(new CompletableFuture<?>[0]))
                .thenApply(unused -> new FilesDeploymentUnit(map));
    }

    @Override
    public void rollback() throws Exception {
        tempStorage.rollback();
    }
}
