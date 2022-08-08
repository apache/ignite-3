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

package org.apache.ignite.internal.raft.server;

import org.jetbrains.annotations.Nullable;

/**
 * Options specific to a Raft group that is being started.
 */
public class RaftGroupOptions {
    /** Whether volatile stores should be used for the corresponding Raft Group. Classic Raft uses persistent ones. */
    private final boolean volatileStores;

    private final @Nullable String volatileLogStoreBudgetClassName;

    /**
     * Returns default options as defined by classic Raft (so stores are persistent).
     *
     * @return Default options.
     */
    public static RaftGroupOptions defaults() {
        return forPersistentStores();
    }

    /**
     * Returns options with persistent Raft stores.
     *
     * @return Options with persistent Raft stores.
     */
    public static RaftGroupOptions forPersistentStores() {
        return new RaftGroupOptions(false, null);
    }

    /**
     * Returns options with volatile Raft stores.
     *
     * @return Options with volatile Raft stores.
     */
    public static RaftGroupOptions forVolatileStores(@Nullable String volatileLogStoreBudgetClassName) {
        return new RaftGroupOptions(true, volatileLogStoreBudgetClassName);
    }

    /**
     * Creates options derived from table configuration.
     *
     * @param isVolatile Whether the table is configured as volatile (in-memory) or not.
     * @return Options derived from table configuration.
     */
    public static RaftGroupOptions forTable(boolean isVolatile, @Nullable String volatileLogStoreBudgetClassName) {
        return isVolatile ? forVolatileStores(volatileLogStoreBudgetClassName) : forPersistentStores();
    }

    private RaftGroupOptions(boolean volatileStores, @Nullable String volatileLogStoreBudgetClassName) {
        this.volatileStores = volatileStores;
        this.volatileLogStoreBudgetClassName = volatileLogStoreBudgetClassName;
    }

    /**
     * Returns {@code true} if the Raft group should store its metadata and logs in volatile storages, or {@code false}
     * if these storages should be persistent.
     *
     * @return {@code true} if the Raft group should store its metadata and logs in volatile storages, or {@code false}
     *     if these storages should be persistent.
     */
    public boolean volatileStores() {
        return volatileStores;
    }

    /**
     * Returns name of a class implementing {@link org.apache.ignite.raft.jraft.storage.impl.LogStorageBudget} that
     * will be used by volatile Raft log storage (if it's used).
     *
     * @return Name of a budget class.
     */
    public @Nullable String volatileLogStoreBudgetClassName() {
        return volatileLogStoreBudgetClassName;
    }
}
