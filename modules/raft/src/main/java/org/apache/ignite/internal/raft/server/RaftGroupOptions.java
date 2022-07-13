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

/**
 * Options specific to a Raft group that is being started.
 */
public class RaftGroupOptions {
    /** Whether volatile stores should be used for the corresponding Raft Group. Classic Raft uses persistent ones. */
    private final boolean volatileStores;

    /**
     * Index of the last processed command according to persisted local storage data. {@code 0} if storage is volatile or has not yet
     * processed any commands.
     */
    private long lastAppliedIndex;

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
        return new RaftGroupOptions(false);
    }

    /**
     * Returns options with volatile Raft stores.
     *
     * @return Options with volatile Raft stores.
     */
    public static RaftGroupOptions forVolatileStores() {
        return new RaftGroupOptions(true);
    }

    /**
     * Creates options derived from table configuration.
     *
     * @param isVolatile Whether the table is configured as volatile (in-memory) or not.
     * @return Options derived from table configuration.
     */
    public static RaftGroupOptions forTable(boolean isVolatile) {
        return isVolatile ? forVolatileStores() : forPersistentStores();
    }

    private RaftGroupOptions(boolean volatileStores) {
        this.volatileStores = volatileStores;
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

    /*
     * Returns an index of the last processed command according to persisted local storage data. {@code 0} if storage is volatile or has not
     * yet processed any commands.
     */
    public long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    /**
     * Sets a value of last applied index.
     * @see #lastAppliedIndex()
     */
    public void lastAppliedIndex(long lastAppliedIndex) {
        this.lastAppliedIndex = lastAppliedIndex;
    }
}
