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

package org.apache.ignite.internal.raft.storage.impl;

import java.nio.file.Path;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.jetbrains.annotations.Nullable;

/** Contains {@link StorageDestructionIntent}, server data path and {@link LogStorageFactory} for storage destruction. */
public class StoragesDestructionContext {
    private final StorageDestructionIntent intent;

    private final @Nullable LogStorageFactory logStorageFactory;
    private final Path serverDataPath;

    /**
     * Constructor.
     *
     * @param intent Intent to destroy raft group storages.
     * @param logStorageFactory factory managing log storage of the group.
     * @param serverDataPath Path containing raft server metadata.
     */
    public StoragesDestructionContext(StorageDestructionIntent intent, @Nullable LogStorageFactory logStorageFactory, Path serverDataPath) {
        this.intent = intent;
        this.logStorageFactory = logStorageFactory;
        this.serverDataPath = serverDataPath;
    }

    /** Returns the path containing raft server metadata. */
    public Path serverDataPath() {
        return serverDataPath;
    }

    /** Returns the factory managing log storage of the group. Null for volatile raft groups on startup. */
    public @Nullable LogStorageFactory logStorageFactory() {
        return logStorageFactory;
    }

    /** Returns the intent to destroy raft group storages. */
    public StorageDestructionIntent intent() {
        return intent;
    }
}
