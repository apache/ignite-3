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

package org.apache.ignite.raft.jraft.core;

import java.nio.file.Paths;

import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.raft.storage.impl.IgniteJraftServiceFactory;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.storage.logit.storage.HybridLogStorage;
import org.apache.ignite.raft.jraft.util.Requires;
import org.apache.ignite.raft.jraft.util.StringUtils;

/** Creates hybrid log storage using passed managers, for smooth migration from old log storage type. */
public class HybridLogJRaftServiceFactory extends IgniteJraftServiceFactory {
    private final LogStorageManager newStorageManager;

    private final String newStorageRelativePath;

    /**
    * Creates a new instance of HybridLogJRaftServiceFactory for migrating from old storage type to new one.
    * @param oldStorageManager Factory to create old log storages.
    * @param newStorageManager Factory to create new log storage.
    */
    public HybridLogJRaftServiceFactory(
            LogStorageManager oldStorageManager,
            LogStorageManager newStorageManager,
            String newStorageRelativePath
    ) {
        super(oldStorageManager);

        this.newStorageManager = newStorageManager;
        this.newStorageRelativePath = newStorageRelativePath;
    }

    @Override
    public LogStorage createLogStorage(String uri, RaftOptions raftOptions) {
        Requires.requireTrue(StringUtils.isNotBlank(uri), "Blank log storage uri.");

        // Create old storage if needed.
        LogStorage oldStorage = null;
        if (raftOptions.isStartupOldStorage()) {
            oldStorage = super.createLogStorage(uri, raftOptions);
        }

        String newStorageAbsolutePath = Paths.get(uri, this.newStorageRelativePath).toString();

        return new HybridLogStorage(newStorageAbsolutePath, raftOptions, oldStorage, createNewLogStorage(newStorageAbsolutePath, raftOptions));
    }

    private LogStorage createNewLogStorage(String uri, RaftOptions raftOptions) {
        return newStorageManager.createLogStorage(uri, raftOptions);
    }
}

