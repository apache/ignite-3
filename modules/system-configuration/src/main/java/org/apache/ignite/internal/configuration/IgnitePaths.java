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
import java.util.function.Supplier;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.internal.util.LazyPath;

/**
 * Manages storage paths for Ignite.
 */
public class IgnitePaths {

    /**
     * Path to the base directory of the partitions data.
     */
    private static final Path PARTITIONS_BASE_PATH = Paths.get("partitions");

    /**
     * Path to the persistent storage used by the VaultService component.
     */
    private static final Path VAULT_DB_PATH = Paths.get("vault");

    /**
     * Path to the persistent storage used by the MetaStorageManager component.
     */
    private static final Path METASTORAGE_DB_PATH = Paths.get("metastorage");

    /**
     * Path to the persistent storage used by the ClusterManagementGroupManager component.
     */
    private static final Path CMG_DB_PATH = Paths.get("cmg");

    /**
     * Path for the persistent storage.
     */
    private static final Path STORAGE_PATH = Paths.get("db");

    /**
     * Path for the raft log.
     */
    private static final Path RAFT_LOG_PATH = Paths.get("log");

    /**
     * Path for the raft metadata (snapshots).
     */
    private static final Path METADATA_PATH = Paths.get("meta");

    /**
     * Directory where partition data is stored. By default "partitions" subfolder of data storage path is used.
     *
     * @param systemConfiguration Configuration to read the configured value for this path.
     * @param workDir Ignite working dir. Will be used as a default place to store partitions dir, if it is not set in the
     *         configuration.
     */
    public static LazyPath partitionsBasePath(SystemConfiguration systemConfiguration, Path workDir) {
        return lazy(systemConfiguration.partitionsBasePath(), () -> workDir.resolve(PARTITIONS_BASE_PATH));
    }

    /**
     * Directory where raft metadata is stored. "meta" dir inside "partitions" dir.
     *
     * @param partitionsBaseDir Directory where partition data is stored. See
     *         {@link #partitionsBasePath(SystemConfiguration, Path)}.
     */
    public static LazyPath partitionsMetaPath(LazyPath partitionsBaseDir) {
        return partitionsBaseDir.resolveLazy(METADATA_PATH);
    }

    /**
     * Directory where raft log is stored. "log" dir inside "partitions" dir.
     *
     * @param partitionsBaseDir Directory where partition data is stored. See
     *         {@link #partitionsBasePath(SystemConfiguration, Path)}.
     */
    public static LazyPath partitionsRaftLogPath(LazyPath partitionsBaseDir) {
        return partitionsBaseDir.resolveLazy(RAFT_LOG_PATH);
    }

    /**
     * Returns a path to the partitions store directory. "db" dir inside "partitions" dir.
     *
     * @param partitionsBaseDir Directory where partition data is stored. See
     *         {@link #partitionsBasePath(SystemConfiguration, Path)}.
     */
    public static LazyPath partitionsStorePath(LazyPath partitionsBaseDir) {
        return partitionsBaseDir.resolveLazy(STORAGE_PATH);
    }

    /**
     * Path to Metastorage store.
     *
     * @param workDir Ignite working dir.
     */
    public static Path metastorageDbPath(Path workDir) {
        return workDir.resolve(METASTORAGE_DB_PATH);
    }

    /**
     * Path to CMG store.
     *
     * @param workDir Ignite working dir.
     */
    public static Path cmgDbPath(Path workDir) {
        return workDir.resolve(CMG_DB_PATH);
    }

    /**
     * Path to Vault store.
     *
     * @param workDir Ignite working dir.
     */
    public static Path vaultPath(Path workDir) {
        return workDir.resolve(VAULT_DB_PATH);
    }

    private IgnitePaths() {
        // No-op.
    }

    private static LazyPath lazy(ConfigurationValue<String> value, Supplier<Path> defaultPath) {
        return LazyPath.create(value::value, defaultPath);
    }
}
