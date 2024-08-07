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

package org.apache.ignite.internal.configuration;


import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.ignite.internal.util.LazyPath;

/**
 * Working dir subtree structure representation for components.
 */
public class ComponentWorkingDir {
    /**
     * Path for the persistent storage.
     */
    private static final Path STORAGE_PATH = Paths.get("db");

    /**
     * Path for the raft log.
     */
    private static final Path RAFT_LOG_PATH = Paths.get("log");

    /**
     * Path for the metadata.
     */
    private static final Path METADATA_PATH = Paths.get("meta");

    private final LazyPath basePath;

    public ComponentWorkingDir(LazyPath basePath) {
        this.basePath = basePath;
    }

    public ComponentWorkingDir(Path basePath) {
        this(LazyPath.create(basePath));
    }

    /**
     * Returns base path for the component.
     */
    public LazyPath basePath() {
        return basePath;
    }

    /**
     * Returns metadata path for the component. A subdirectory in the base path.
     */
    public LazyPath metaPath() {
        return basePath.resolveLazy(METADATA_PATH);
    }

    /**
     * Returns raft log path for the component. A subdirectory in the base path.
     */
    public LazyPath raftLogPath() {
        return basePath.resolveLazy(RAFT_LOG_PATH);
    }

    /**
     * Returns storage path for the component. A subdirectory in the base path.
     */
    public LazyPath dbPath() {
        return basePath.resolveLazy(STORAGE_PATH);
    }
}
