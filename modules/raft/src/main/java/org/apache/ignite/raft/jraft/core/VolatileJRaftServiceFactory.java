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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Supplier;
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

    private final Map<String, Supplier<LogStorageBudget>> budgetFactories;

    public VolatileJRaftServiceFactory(RaftGroupOptions groupOptions) {
        this.groupOptions = groupOptions;

        Map<String, Supplier<LogStorageBudget>> factories = new HashMap<>();

        ClassLoader serviceClassLoader = Thread.currentThread().getContextClassLoader();
        for (LogStorageBudgetsModule module : ServiceLoader.load(LogStorageBudgetsModule.class, serviceClassLoader)) {
            Map<String, Supplier<LogStorageBudget>> factoriesFromModule = module.budgetFactories();

            checkForBudgetNameClashes(factories.keySet(), factoriesFromModule.keySet());

            factories.putAll(factoriesFromModule);
        }

        budgetFactories = Map.copyOf(factories);
    }

    private void checkForBudgetNameClashes(Set<String> names1, Set<String> names2) {
        Set<String> intersection = new HashSet<>(names1);
        intersection.retainAll(names2);

        if (!intersection.isEmpty()) {
            throw new IgniteInternalException(
                    String.format("Storage budget '%s' is provided by more than one module", intersection.iterator().next())
            );
        }
    }

    @Override
    public LogStorage createLogStorage(final String groupId, final RaftOptions raftOptions) {
        Requires.requireTrue(StringUtils.isNotBlank(groupId), "Blank group id.");

        return new VolatileLogStorage(createLogStorageBudget());
    }

    private LogStorageBudget createLogStorageBudget() {
        if (groupOptions.volatileLogStoreBudgetName() != null) {
            return newBudget(groupOptions.volatileLogStoreBudgetName());
        } else {
            return new UnlimitedBudget();
        }
    }

    private LogStorageBudget newBudget(String budgetName) {
        Supplier<LogStorageBudget> factory = budgetFactories.get(budgetName);

        if (factory == null) {
            throw new IgniteInternalException("Cannot find a log storage budget by name '" + budgetName + "'");
        }

        return factory.get();
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
