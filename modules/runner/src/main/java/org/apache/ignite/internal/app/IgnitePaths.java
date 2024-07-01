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

package org.apache.ignite.internal.app;

import static org.apache.ignite.internal.raft.util.ConfigurationPathUtils.pathOrDefaultSupplier;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Supplier;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;

/**
 * Manages metadata paths.
 */
public class IgnitePaths {

    /**
     * Path to the persistent storage used by the {@link MetaStorageManager} component.
     */
    private static final Path METASTORAGE_DB_PATH = Paths.get("metastorage");

    /**
     * Path to the persistent storage used by the {@link ClusterManagementGroupManager} component.
     */
    private static final Path CMG_DB_PATH = Paths.get("cmg");

    public static Supplier<Path> cmgWorkDir(RaftConfiguration raftConfiguration, Path workDir) {
        return pathOrDefaultSupplier(raftConfiguration.cmgPath(), () -> workDir.resolve(CMG_DB_PATH));
    }

    public static Supplier<Path> cmgRaftLogDir(RaftConfiguration raftConfiguration, Supplier<Path> defaultPath) {
        return pathOrDefaultSupplier(raftConfiguration.cmgLogPath(), () -> defaultPath.get().resolve("log"));
    }

    public static Supplier<Path> cmgDbDir(RaftConfiguration raftConfiguration, Supplier<Path> defaultPath) {
        return pathOrDefaultSupplier(raftConfiguration.cmgDbPath(), () -> defaultPath.get().resolve("db"));
    }

    public static Supplier<Path> metastorageWorkDir(RaftConfiguration raftConfiguration, Path workDir) {
        return pathOrDefaultSupplier(raftConfiguration.metastoragePath(), () -> workDir.resolve(METASTORAGE_DB_PATH));
    }

    public static Supplier<Path> metastorageRaftLogDir(RaftConfiguration raftConfiguration, Supplier<Path> defaultPath) {
        return pathOrDefaultSupplier(raftConfiguration.metastorageLogPath(), () -> defaultPath.get().resolve("log"));
    }

    public static Supplier<Path> metastorageDbDir(RaftConfiguration raftConfiguration, Supplier<Path> defaultPath) {
        return pathOrDefaultSupplier(raftConfiguration.metastorageDbPath(), () -> defaultPath.get().resolve("db"));
    }
}
