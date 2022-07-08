/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.core;

import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.raft.jraft.JRaftServiceFactory;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.storage.RaftMetaStorage;
import org.apache.ignite.raft.jraft.storage.SnapshotStorage;
import org.apache.ignite.raft.jraft.storage.impl.LogStorageBudget;
import org.apache.ignite.raft.jraft.storage.impl.UnlimitedBudget;
import org.apache.ignite.raft.jraft.storage.impl.VolatileLogStorage;
import org.apache.ignite.raft.jraft.storage.impl.VolatileRaftMetaStorage;
import org.apache.ignite.raft.jraft.storage.snapshot.local.LocalSnapshotStorage;
import org.apache.ignite.raft.jraft.util.Requires;
import org.apache.ignite.raft.jraft.util.StringUtils;

/**
 * The factory for JRaft services producing volatile stores. Useful for Raft groups hosting partitions of in-memory tables.
 */
public class VolatileJRaftServiceFactory implements JRaftServiceFactory {
    private final RaftGroupOptions groupOptions;

    public VolatileJRaftServiceFactory(RaftGroupOptions groupOptions) {
        this.groupOptions = groupOptions;
    }

    @Override
    public LogStorage createLogStorage(final String groupId, final RaftOptions raftOptions) {
        Requires.requireTrue(StringUtils.isNotBlank(groupId), "Blank group id.");

        return new VolatileLogStorage(createLogStorageBudget());
    }

    private LogStorageBudget createLogStorageBudget() {
        if (groupOptions.volatileLogStoreBudgetClassName() != null) {
            String className = groupOptions.volatileLogStoreBudgetClassName();
            return instantiate(className);
        } else {
            return new UnlimitedBudget();
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T instantiate(String className) {
        try {
            return (T) Class.forName(className).getConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IgniteInternalException("Cannot instantiate '" + className + "'", e);
        }
    }

    @Override
    public SnapshotStorage createSnapshotStorage(final String uri, final RaftOptions raftOptions) {
        Requires.requireTrue(!StringUtils.isBlank(uri), "Blank snapshot storage uri.");

        // TODO: IGNITE-17083 - return an in-memory store here (or get rid of SnapshotStorage)

        return new LocalSnapshotStorage(uri, raftOptions);
    }

    @Override
    public RaftMetaStorage createRaftMetaStorage(final String uri, final RaftOptions raftOptions) {
        return new VolatileRaftMetaStorage();
    }
}
