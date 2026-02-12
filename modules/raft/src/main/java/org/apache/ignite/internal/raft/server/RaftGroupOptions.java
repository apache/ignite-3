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
import org.apache.ignite.internal.raft.storage.LogStorageFactory;
import org.apache.ignite.internal.raft.storage.RaftMetaStorageFactory;
import org.apache.ignite.internal.raft.storage.SnapshotStorageFactory;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.SafeTimeValidator;
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

    /** Marshaller to marshall/unmarshall commands. */
    private @Nullable Marshaller commandsMarshaller;

    /** Path to store raft data. */
    private @Nullable Path serverDataPath;

    /**
     * Externally enforced config index.
     *
     * @see #externallyEnforcedConfigIndex()
     */
    private @Nullable Long externallyEnforcedConfigIndex;

    /**
     * Max clock skew in the replication group in milliseconds.
     */
    private int maxClockSkewMs;

    /**
     * If the group is declared as a system group, certain threads are dedicated specifically for that one.
     */
    private boolean isSystemGroup = false;

    private @Nullable SafeTimeValidator safeTimeValidator;

    /**
     * Gets a system group flag.
     *
     * @return System group flag.
     */
    public boolean isSystemGroup() {
        return isSystemGroup;
    }

    /**
     * Sets a system flag.
     * If the flag is true, some resources are used in an exclusive manner to avoid collision with data flow.
     * Otherwise, the RAFT group shares resources to save physical machine capacity.
     *
     * @param systemGroup True for system group, false for client data.
     * @return System group flag.
     */
    public RaftGroupOptions setSystemGroup(boolean systemGroup) {
        isSystemGroup = systemGroup;

        return this;
    }

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

    /**
     * Externally enforced config index.
     *
     * <p>If it's not {@code null}, then the Raft node abstains from becoming a leader in configurations whose index precedes
     * the externally enforced index..
     *
     * <p>The idea is that, if a Raft group was forcefully repaired (because it lost majority) using resetPeers(),
     * the old majority nodes might come back online. If this happens and we do nothing, they might elect a leader from the old majority
     * that could hijack leadership and cause havoc in the repaired group.
     *
     * <p>To prevent this, on a starup or subsequent config changes, current voting set (aka peers) of the repaired group may be 'broken'
     * to make it impossible for the current node to become a leader. This is enabled by setting a non-null value to
     * {@link NodeOptions#getExternallyEnforcedConfigIndex ()}. When it's set, on each change of configuration (happening to this.conf),
     * including the one at startup, we check whether the applied config precedes the externally enforced
     * config (in which case this.conf.peers will be 'broken' to make sure current node does not become a leader) or not (in which case
     * the applied config will be used as is).
     */
    public @Nullable Long externallyEnforcedConfigIndex() {
        return externallyEnforcedConfigIndex;
    }

    /**
     * Sets externally enforced config index for the group.
     *
     * @param index Index to set.
     * @return This object.
     * @see #externallyEnforcedConfigIndex()
     */
    public RaftGroupOptions externallyEnforcedConfigIndex(@Nullable Long index) {
        externallyEnforcedConfigIndex = index;
        return this;
    }

    /**
     * Set max clock skew.
     *
     * @param maxClockSkewMs The skew in milliseconds.
     * @return This object.
     */
    public RaftGroupOptions maxClockSkew(int maxClockSkewMs) {
        this.maxClockSkewMs = maxClockSkewMs;
        return this;
    }

    /**
     * Get max clock skew.
     *
     * @return The skew.
     */
    public int maxClockSkew() {
        return maxClockSkewMs;
    }

    public @Nullable SafeTimeValidator safeTimeValidator() {
        return safeTimeValidator;
    }

    public RaftGroupOptions safeTimeValidator(@Nullable SafeTimeValidator safeTimeValidator) {
        this.safeTimeValidator = safeTimeValidator;
        return this;
    }
}
