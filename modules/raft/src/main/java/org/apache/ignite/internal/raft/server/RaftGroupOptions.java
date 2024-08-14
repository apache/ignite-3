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

package org.apache.ignite.internal.raft.server;

import java.nio.file.Path;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.RaftNodeDisruptorConfiguration;
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.storage.RaftMetaStorageFactory;
import org.apache.ignite.internal.raft.storage.SnapshotStorageFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Options specific to a Raft group that is being started.
 */
public class RaftGroupOptions {
    /** Whether volatile stores should be used for the corresponding Raft Group. Classic Raft uses persistent ones. */
    private final boolean volatileStores;

    /** Log storage factory. */
    private LogStorageFactory logStorageFactory;

    /** Snapshot storage factory. */
    private @Nullable SnapshotStorageFactory snapshotStorageFactory;

    /** Raft meta storage factory. */
    private RaftMetaStorageFactory raftMetaStorageFactory;

    /** Configuration of own striped disruptor for FSMCaller service of raft node, {@code null} means use shared disruptor. */
    private @Nullable RaftNodeDisruptorConfiguration ownFsmCallerExecutorDisruptorConfig;

    /** Marshaller to marshall/unmarshall commands. */
    private @Nullable Marshaller commandsMarshaller;

    /** Path to store raft data. */
    private @Nullable Path serverDataPath;

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

    /**
     * Returns a log storage factory that's used to create log storage for a raft group.
     */
    public LogStorageFactory getLogStorageFactory() {
        return logStorageFactory;
    }

    /**
     * Adds log storage factory to options.
     */
    public RaftGroupOptions setLogStorageFactory(LogStorageFactory logStorageFactory) {
        this.logStorageFactory = logStorageFactory;

        return this;
    }

    /**
     * Returns a snapshot storage factory that's used to create snapshot storage for a raft group.
     */
    public @Nullable SnapshotStorageFactory snapshotStorageFactory() {
        return snapshotStorageFactory;
    }

    /**
     * Adds snapshot storage factory to options.
     */
    public RaftGroupOptions snapshotStorageFactory(@Nullable SnapshotStorageFactory snapshotStorageFactory) {
        this.snapshotStorageFactory = snapshotStorageFactory;

        return this;
    }

    /**
     * Returns a raft meta storage factory that's used to create raft meta storage for a raft group.
     */
    public RaftMetaStorageFactory raftMetaStorageFactory() {
        return raftMetaStorageFactory;
    }

    /**
     * Adds raft meta storage factory to options.
     */
    public RaftGroupOptions raftMetaStorageFactory(RaftMetaStorageFactory raftMetaStorageFactory) {
        this.raftMetaStorageFactory = raftMetaStorageFactory;

        return this;
    }

    /**
     * Returns configuration of own striped disruptor for FSMCaller service of raft node, {@code null} means use shared disruptor.
     */
    public @Nullable RaftNodeDisruptorConfiguration ownFsmCallerExecutorDisruptorConfig() {
        return ownFsmCallerExecutorDisruptorConfig;
    }

    /**
     * Sets configuration of own striped disruptor for FSMCaller service of raft node.
     */
    public RaftGroupOptions ownFsmCallerExecutorDisruptorConfig(RaftNodeDisruptorConfiguration ownFsmCallerExecutorDisruptorConfig) {
        this.ownFsmCallerExecutorDisruptorConfig = ownFsmCallerExecutorDisruptorConfig;

        return this;
    }

    /**
     * Returns Marshaller used to marshall/unmarshall commands.
     */
    public @Nullable Marshaller commandsMarshaller() {
        return commandsMarshaller;
    }

    /**
     * Sets the marshaller to use with commands.
     *
     * @param marshaller Marshaller.
     * @return This object.
     */
    public RaftGroupOptions commandsMarshaller(Marshaller marshaller) {
        commandsMarshaller = marshaller;

        return this;
    }

    /**
     * Returns path to store raft data.
     */
    public @Nullable Path serverDataPath() {
        return serverDataPath;
    }

    /**
     * Sets path to store raft data.
     *
     * @param serverDataPath Path
     * @return This object.
     */
    public RaftGroupOptions serverDataPath(Path serverDataPath) {
        this.serverDataPath = serverDataPath;

        return this;
    }

}
