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

import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.raft.storage.RaftMetaStorageFactory;
import org.apache.ignite.internal.raft.storage.SnapshotStorageFactory;
import org.apache.ignite.raft.jraft.core.DefaultJRaftServiceFactory;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.storage.RaftMetaStorage;
import org.apache.ignite.raft.jraft.storage.SnapshotStorage;
import org.apache.ignite.raft.jraft.storage.impl.LocalRaftMetaStorage;
import org.apache.ignite.raft.jraft.storage.snapshot.local.LocalSnapshotStorage;
import org.apache.ignite.raft.jraft.util.Requires;
import org.apache.ignite.raft.jraft.util.StringUtils;
import org.jetbrains.annotations.TestOnly;

/**
 * The default factory for JRaft services for Ignite.
 */
public class IgniteJraftServiceFactory extends DefaultJRaftServiceFactory {
    /** Log storage manager .*/
    private final LogStorageManager logStorageManager;

    /** Snapshot storage factory. */
    private volatile SnapshotStorageFactory snapshotStorageFactory = LocalSnapshotStorage::new;

    /** Raft meta storage factory. */
    private volatile RaftMetaStorageFactory raftMetaStorageFactory = LocalRaftMetaStorage::new;

    public IgniteJraftServiceFactory(LogStorageManager factory) {
        logStorageManager = factory;
    }

    /**
     * Updates a value of {@link SnapshotStorageFactory} in this service factory.
     */
    public IgniteJraftServiceFactory setSnapshotStorageFactory(SnapshotStorageFactory snapshotStorageFactory) {
        this.snapshotStorageFactory = snapshotStorageFactory;

        return this;
    }

    /**
     * Updates a value of {@link RaftMetaStorageFactory} in this service factory.
     */
    public IgniteJraftServiceFactory setRaftMetaStorageFactory(RaftMetaStorageFactory raftMetaStorageFactory) {
        this.raftMetaStorageFactory = raftMetaStorageFactory;

        return this;
    }

    @Override
    public LogStorage createLogStorage(final String groupId, final RaftOptions raftOptions) {
        Requires.requireTrue(StringUtils.isNotBlank(groupId), "Blank group id.");

        return logStorageManager.createLogStorage(groupId, raftOptions);
    }

    @Override
    public SnapshotStorage createSnapshotStorage(final String uri, final RaftOptions raftOptions) {
        Requires.requireTrue(!StringUtils.isBlank(uri), "Blank snapshot storage uri.");

        return snapshotStorageFactory.createSnapshotStorage(uri, raftOptions);
    }

    @Override
    public RaftMetaStorage createRaftMetaStorage(final String uri, final RaftOptions raftOptions) {
        Requires.requireTrue(!StringUtils.isBlank(uri), "Blank raft meta storage uri.");

        return raftMetaStorageFactory.createRaftMetaStorage(uri, raftOptions);
    }

    /** Returns {@link LogStorageManager}. */
    @TestOnly
    public LogStorageManager logStorageManager() {
        return logStorageManager;
    }
}
