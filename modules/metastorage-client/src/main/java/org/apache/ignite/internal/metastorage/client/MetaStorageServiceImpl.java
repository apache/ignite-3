/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.metastorage.client;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.metastorage.common.command.GetAllCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndPutAllCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndPutCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndRemoveAllCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndRemoveCommand;
import org.apache.ignite.internal.metastorage.common.command.GetCommand;
import org.apache.ignite.internal.metastorage.common.command.PutAllCommand;
import org.apache.ignite.internal.metastorage.common.command.PutCommand;
import org.apache.ignite.internal.metastorage.common.command.RemoveAllCommand;
import org.apache.ignite.internal.metastorage.common.command.RemoveCommand;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.metastorage.client.MetaStorageService;
import org.apache.ignite.metastorage.common.Condition;
import org.apache.ignite.metastorage.common.Cursor;
import org.apache.ignite.metastorage.common.Entry;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.Operation;
import org.apache.ignite.metastorage.common.WatchListener;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link MetaStorageService} implementation.
 */
public class MetaStorageServiceImpl implements MetaStorageService {
    /** Meta storage raft group service. */
    private final RaftGroupService metaStorageRaftGrpSvc;

    /**
     * @param metaStorageRaftGroupSvc Meta storage raft group service.
     */
    MetaStorageServiceImpl(RaftGroupService metaStorageRaftGroupSvc) {
        this.metaStorageRaftGrpSvc = metaStorageRaftGroupSvc;
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Entry> get(@NotNull Key key) {
        return metaStorageRaftGrpSvc.<Entry>run(new GetCommand(key))
            .thenApply(response -> response);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Entry> get(@NotNull Key key, long revUpperBound) {
        return metaStorageRaftGrpSvc.<Entry>run(new GetCommand(key, revUpperBound))
            .thenApply(response -> response);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Map<Key, Entry>> getAll(Collection<Key> keys) {
        return metaStorageRaftGrpSvc.<Map<Key, Entry>>run(new GetAllCommand(keys))
            .thenApply(response -> response);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Map<Key, Entry>> getAll(Collection<Key> keys, long revUpperBound) {
        return metaStorageRaftGrpSvc.<Map<Key, Entry>>run(new GetAllCommand(keys, revUpperBound))
            .thenApply(response -> response);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> put(@NotNull Key key, @NotNull byte[] value) {
        return metaStorageRaftGrpSvc.<Void>run(new PutCommand(key, value))
            .thenApply(response -> response);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Entry> getAndPut(@NotNull Key key, @NotNull byte[] value) {
        return metaStorageRaftGrpSvc.<Entry>run(new GetAndPutCommand(key, value))
            .thenApply(response -> response);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> putAll(@NotNull Map<Key, byte[]> vals) {
        return metaStorageRaftGrpSvc.<Void>run(new PutAllCommand(vals))
            .thenApply(response -> response);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Map<Key, Entry>> getAndPutAll(@NotNull Map<Key, byte[]> vals) {
        return metaStorageRaftGrpSvc.<Map<Key, Entry>>run(new GetAndPutAllCommand(vals))
            .thenApply(response -> response);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> remove(@NotNull Key key) {
        return metaStorageRaftGrpSvc.<Void>run(new RemoveCommand(key))
            .thenApply(response -> response);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Entry> getAndRemove(@NotNull Key key) {
        return metaStorageRaftGrpSvc.<Entry>run(new GetAndRemoveCommand(key))
            .thenApply(response -> response);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> removeAll(@NotNull Collection<Key> keys) {
        return metaStorageRaftGrpSvc.<Void>run(new RemoveAllCommand(keys))
            .thenApply(response -> response);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Map<Key, Entry>> getAndRemoveAll(@NotNull Collection<Key> keys) {
        return metaStorageRaftGrpSvc.<Map<Key, Entry>>run(new GetAndRemoveAllCommand(keys))
            .thenApply(response -> response);
    }

    @Override public @NotNull CompletableFuture<Boolean> invoke(@NotNull Key key, @NotNull Condition condition,
        @NotNull Operation success, @NotNull Operation failure) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Entry> getAndInvoke(@NotNull Key key, @NotNull Condition condition,
        @NotNull Operation success, @NotNull Operation failure) {
        return null;
    }

    @Override public @NotNull Cursor<Entry> range(@NotNull Key keyFrom, @Nullable Key keyTo, long revUpperBound) {
        return null;
    }

    @Override public @NotNull Cursor<Entry> range(@NotNull Key keyFrom, @Nullable Key keyTo) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<IgniteUuid> watch(@Nullable Key keyFrom, @Nullable Key keyTo, long revision,
        @NotNull WatchListener lsnr) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<IgniteUuid> watch(@NotNull Key key, long revision, @NotNull WatchListener lsnr) {
        return null;
    }

    @Override public @NotNull CompletableFuture<IgniteUuid> watch(@NotNull Collection<Key> keys, long revision,
        @NotNull WatchListener lsnr) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Void> stopWatch(@NotNull IgniteUuid id) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Void> compact() {
        return null;
    }
}
