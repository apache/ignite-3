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

package org.apache.ignite.internal.metastorage.impl;

import static org.apache.ignite.internal.metastorage.command.GetAllCommand.getAllCommand;
import static org.apache.ignite.internal.metastorage.command.GetAndPutAllCommand.getAndPutAllCommand;
import static org.apache.ignite.internal.metastorage.command.GetAndRemoveAllCommand.getAndRemoveAllCommand;
import static org.apache.ignite.internal.metastorage.command.PutAllCommand.putAllCommand;
import static org.apache.ignite.internal.metastorage.command.RemoveAllCommand.removeAllCommand;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.command.GetAllCommand;
import org.apache.ignite.internal.metastorage.command.GetAndPutAllCommand;
import org.apache.ignite.internal.metastorage.command.GetAndPutCommand;
import org.apache.ignite.internal.metastorage.command.GetAndRemoveAllCommand;
import org.apache.ignite.internal.metastorage.command.GetAndRemoveCommand;
import org.apache.ignite.internal.metastorage.command.GetCommand;
import org.apache.ignite.internal.metastorage.command.InvokeCommand;
import org.apache.ignite.internal.metastorage.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.metastorage.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.command.PutAllCommand;
import org.apache.ignite.internal.metastorage.command.PutCommand;
import org.apache.ignite.internal.metastorage.command.RemoveAllCommand;
import org.apache.ignite.internal.metastorage.command.RemoveCommand;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * {@link MetaStorageService} implementation.
 */
public class MetaStorageServiceImpl implements MetaStorageService {
    private static final IgniteLogger LOG = Loggers.forClass(MetaStorageService.class);

    private final MetaStorageServiceContext context;

    /** Local node. */
    private final ClusterNode localNode;

    /**
     * Constructor.
     *
     * @param metaStorageRaftGrpSvc Meta storage raft group service.
     * @param localNode Local node.
     */
    public MetaStorageServiceImpl(RaftGroupService metaStorageRaftGrpSvc, IgniteSpinBusyLock busyLock, ClusterNode localNode) {
        this.context = new MetaStorageServiceContext(
                metaStorageRaftGrpSvc,
                new MetaStorageCommandsFactory(),
                // TODO: Extract the pool size into configuration, see https://issues.apache.org/jira/browse/IGNITE-18735
                Executors.newFixedThreadPool(5, NamedThreadFactory.create(localNode.name(), "metastorage-publisher", LOG)),
                busyLock
        );

        this.localNode = localNode;
    }

    RaftGroupService raftGroupService() {
        return context.raftService();
    }

    @Override
    public CompletableFuture<Entry> get(ByteArray key) {
        GetCommand getCommand = context.commandsFactory().getCommand().key(key.bytes()).build();

        return context.raftService().run(getCommand);
    }

    @Override
    public CompletableFuture<Entry> get(ByteArray key, long revUpperBound) {
        GetCommand getCommand = context.commandsFactory().getCommand().key(key.bytes()).revision(revUpperBound).build();

        return context.raftService().run(getCommand);
    }

    @Override
    public CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys) {
        GetAllCommand getAllCommand = getAllCommand(context.commandsFactory(), keys, 0);

        return context.raftService().<List<Entry>>run(getAllCommand)
                .thenApply(MetaStorageServiceImpl::multipleEntryResult);
    }

    @Override
    public CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys, long revUpperBound) {
        GetAllCommand getAllCommand = getAllCommand(context.commandsFactory(), keys, revUpperBound);

        return context.raftService().<List<Entry>>run(getAllCommand)
                .thenApply(MetaStorageServiceImpl::multipleEntryResult);
    }

    @Override
    public CompletableFuture<Void> put(ByteArray key, byte[] value) {
        PutCommand putCommand = context.commandsFactory().putCommand().key(key.bytes()).value(value).build();

        return context.raftService().run(putCommand);
    }

    @Override
    public CompletableFuture<Entry> getAndPut(ByteArray key, byte[] value) {
        GetAndPutCommand getAndPutCommand = context.commandsFactory().getAndPutCommand().key(key.bytes()).value(value).build();

        return context.raftService().run(getAndPutCommand);
    }

    @Override
    public CompletableFuture<Void> putAll(Map<ByteArray, byte[]> vals) {
        PutAllCommand putAllCommand = putAllCommand(context.commandsFactory(), vals);

        return context.raftService().run(putAllCommand);
    }

    @Override
    public CompletableFuture<Map<ByteArray, Entry>> getAndPutAll(Map<ByteArray, byte[]> vals) {
        GetAndPutAllCommand getAndPutAllCommand = getAndPutAllCommand(context.commandsFactory(), vals);

        return context.raftService().<List<Entry>>run(getAndPutAllCommand)
                .thenApply(MetaStorageServiceImpl::multipleEntryResult);
    }

    @Override
    public CompletableFuture<Void> remove(ByteArray key) {
        RemoveCommand removeCommand = context.commandsFactory().removeCommand().key(key.bytes()).build();

        return context.raftService().run(removeCommand);
    }

    @Override
    public CompletableFuture<Entry> getAndRemove(ByteArray key) {
        GetAndRemoveCommand getAndRemoveCommand = context.commandsFactory().getAndRemoveCommand().key(key.bytes()).build();

        return context.raftService().run(getAndRemoveCommand);
    }

    @Override
    public CompletableFuture<Void> removeAll(Set<ByteArray> keys) {
        RemoveAllCommand removeAllCommand = removeAllCommand(context.commandsFactory(), keys);

        return context.raftService().run(removeAllCommand);
    }

    @Override
    public CompletableFuture<Map<ByteArray, Entry>> getAndRemoveAll(Set<ByteArray> keys) {
        GetAndRemoveAllCommand getAndRemoveAllCommand = getAndRemoveAllCommand(context.commandsFactory(), keys);

        return context.raftService().<List<Entry>>run(getAndRemoveAllCommand)
                .thenApply(MetaStorageServiceImpl::multipleEntryResult);
    }

    @Override
    public CompletableFuture<Boolean> invoke(Condition condition, Operation success, Operation failure) {
        return invoke(condition, List.of(success), List.of(failure));
    }

    @Override
    public CompletableFuture<Boolean> invoke(
            Condition condition,
            Collection<Operation> success,
            Collection<Operation> failure
    ) {
        InvokeCommand invokeCommand = context.commandsFactory().invokeCommand()
                .condition(condition)
                .success(success)
                .failure(failure)
                .build();

        return context.raftService().run(invokeCommand);
    }

    @Override
    public CompletableFuture<StatementResult> invoke(Iif iif) {
        MultiInvokeCommand multiInvokeCommand = context.commandsFactory().multiInvokeCommand()
                .iif(iif)
                .build();

        return context.raftService().run(multiInvokeCommand);
    }

    @Override
    public Publisher<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo, long revUpperBound) {
        return range(keyFrom, keyTo, revUpperBound, false);
    }

    @Override
    public Publisher<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo) {
        return range(keyFrom, keyTo, false);
    }

    @Override
    public Publisher<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo, boolean includeTombstones) {
        return range(keyFrom, keyTo, -1, includeTombstones);
    }

    @Override
    public Publisher<Entry> range(
            ByteArray keyFrom,
            @Nullable ByteArray keyTo,
            long revUpperBound,
            boolean includeTombstones
    ) {
        Function<IgniteUuid, WriteCommand> createRangeCommand = cursorId -> context.commandsFactory().createRangeCursorCommand()
                .keyFrom(keyFrom.bytes())
                .keyTo(keyTo == null ? null : keyTo.bytes())
                .revUpperBound(revUpperBound)
                .requesterNodeId(localNode.id())
                .cursorId(cursorId)
                .includeTombstones(includeTombstones)
                .build();

        return new CursorPublisher(context, createRangeCommand);
    }

    @Override
    public Publisher<Entry> prefix(ByteArray prefix, long revUpperBound) {
        Function<IgniteUuid, WriteCommand> createPrefixCommand = cursorId -> context.commandsFactory().createPrefixCursorCommand()
                .prefix(prefix.bytes())
                .revUpperBound(revUpperBound)
                .requesterNodeId(localNode.id())
                .cursorId(cursorId)
                .includeTombstones(false)
                .build();

        return new CursorPublisher(context, createPrefixCommand);
    }

    // TODO: IGNITE-14734 Implement.
    @Override
    public CompletableFuture<Void> compact() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> closeCursors(String nodeId) {
        return context.raftService().run(context.commandsFactory().closeAllCursorsCommand().nodeId(nodeId).build());
    }

    @Override
    public void close() {
        context.close();
    }

    private static Map<ByteArray, Entry> multipleEntryResult(List<Entry> entries) {
        Map<ByteArray, Entry> res = IgniteUtils.newHashMap(entries.size());

        for (Entry e : entries) {
            res.put(new ByteArray(e.key()), e);
        }

        return res;
    }
}
