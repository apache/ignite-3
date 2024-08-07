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
     * Directory where partition data is stored. By default "partitions" subfolder of data storage path is used.
     *
     * @param systemConfiguration System configuration.
     * @param workDir Ignite working dir. Will be used as a default place to store partitions dir, if it is not set in the
     *         configuration.
     * @return Working dir subtree structure representation for partitions.
     */
    public static ComponentWorkingDir partitionsPath(SystemLocalConfiguration systemConfiguration, Path workDir) {
        Path basePath = pathOrDefault(systemConfiguration.partitionsBasePath(), () -> workDir.resolve(PARTITIONS_BASE_PATH));

        return new ComponentWorkingDir(basePath) {
            @Override
            public Path raftLogPath() {
                return pathOrDefault(systemConfiguration.partitionsLogPath(), super::raftLogPath);
            }
        };
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

    private static Path pathOrDefault(ConfigurationValue<String> value, Supplier<Path> defaultPathSupplier) {
        String valueStr = value.value();
        return valueStr.isEmpty() ? defaultPathSupplier.get() : Path.of(valueStr);
    }
}
