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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.command.CompactionCommand;
import org.apache.ignite.internal.metastorage.command.EvictIdempotentCommandsCacheCommand;
import org.apache.ignite.internal.metastorage.command.GetAllCommand;
import org.apache.ignite.internal.metastorage.command.GetChecksumCommand;
import org.apache.ignite.internal.metastorage.command.GetCommand;
import org.apache.ignite.internal.metastorage.command.GetCurrentRevisionsCommand;
import org.apache.ignite.internal.metastorage.command.InvokeCommand;
import org.apache.ignite.internal.metastorage.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.metastorage.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.command.PutAllCommand;
import org.apache.ignite.internal.metastorage.command.PutCommand;
import org.apache.ignite.internal.metastorage.command.RemoveAllCommand;
import org.apache.ignite.internal.metastorage.command.RemoveByPrefixCommand;
import org.apache.ignite.internal.metastorage.command.RemoveCommand;
import org.apache.ignite.internal.metastorage.command.SyncTimeCommand;
import org.apache.ignite.internal.metastorage.command.response.ChecksumInfo;
import org.apache.ignite.internal.metastorage.command.response.RevisionsInfo;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * {@link MetaStorageService} implementation.
 */
public class MetaStorageServiceImpl implements MetaStorageService {
    private static final IgniteLogger LOG = Loggers.forClass(MetaStorageService.class);

    /** Default batch size that is requested from the remote server. */
    public static final int BATCH_SIZE = 1000;

    // TODO: https://issues.apache.org/jira/browse/IGNITE-26085 Use proper timeout or reactive approach.
    /** Timeout for meta storage raft commands processing. */
    private static final int TIMEOUT_MILLIS = 30_000;

    private final MetaStorageServiceContext context;

    private final HybridClock clock;

    private final CommandIdGenerator commandIdGenerator;

    /**
     * Constructor.
     *
     * @param metaStorageRaftGrpSvc Meta storage raft group service.
     */
    public MetaStorageServiceImpl(
            String nodeName,
            RaftGroupService metaStorageRaftGrpSvc,
            IgniteSpinBusyLock busyLock,
            HybridClock clock,
            UUID localNodeId
    ) {
        this.context = new MetaStorageServiceContext(
                metaStorageRaftGrpSvc,
                new MetaStorageCommandsFactory(),
                // TODO: Extract the pool size into configuration, see https://issues.apache.org/jira/browse/IGNITE-18735
                Executors.newFixedThreadPool(5, IgniteThreadFactory.create(nodeName, "metastorage-publisher", LOG)),
                busyLock
        );

        this.clock = clock;
        this.commandIdGenerator = new CommandIdGenerator(localNodeId);
    }

    public RaftGroupService raftGroupService() {
        return context.raftService();
    }

    @Override
    public CompletableFuture<Entry> get(ByteArray key) {
        return get(key, MetaStorageManager.LATEST_REVISION);
    }

    @Override
    public CompletableFuture<Entry> get(ByteArray key, long revUpperBound) {
        GetCommand getCommand = context.commandsFactory().getCommand().key(ByteBuffer.wrap(key.bytes())).revision(revUpperBound).build();

        return context.raftService().run(getCommand, TIMEOUT_MILLIS);
    }

    @Override
    public CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys) {
        return getAll(keys, MetaStorageManager.LATEST_REVISION);
    }

    @Override
    public CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys, long revUpperBound) {
        GetAllCommand getAllCommand = getAllCommand(context.commandsFactory(), keys, revUpperBound);

        return context.raftService().<List<Entry>>run(getAllCommand)
                .thenApply(MetaStorageServiceImpl::multipleEntryResult);
    }

    @Override
    public CompletableFuture<Void> put(ByteArray key, byte[] value) {
        PutCommand putCommand = context.commandsFactory().putCommand()
                .key(ByteBuffer.wrap(key.bytes()))
                .value(ByteBuffer.wrap(value))
                .initiatorTime(clock.now())
                .build();

        return context.raftService().run(putCommand, TIMEOUT_MILLIS);
    }

    @Override
    public CompletableFuture<Void> putAll(Map<ByteArray, byte[]> vals) {
        PutAllCommand putAllCommand = putAllCommand(context.commandsFactory(), vals, clock.now());

        return context.raftService().run(putAllCommand, TIMEOUT_MILLIS);
    }

    @Override
    public CompletableFuture<Void> remove(ByteArray key) {
        RemoveCommand removeCommand = context.commandsFactory().removeCommand().key(ByteBuffer.wrap(key.bytes()))
                .initiatorTime(clock.now()).build();

        return context.raftService().run(removeCommand, TIMEOUT_MILLIS);
    }

    @Override
    public CompletableFuture<Void> removeAll(Set<ByteArray> keys) {
        RemoveAllCommand removeAllCommand = removeAllCommand(context.commandsFactory(), keys, clock.now());

        return context.raftService().run(removeAllCommand, TIMEOUT_MILLIS);
    }

    @Override
    public CompletableFuture<Void> removeByPrefix(ByteArray prefix) {
        RemoveByPrefixCommand removeByPrefix = context.commandsFactory().removeByPrefixCommand()
                .prefix(ByteBuffer.wrap(prefix.bytes()))
                .initiatorTime(clock.now())
                .build();

        return context.raftService().run(removeByPrefix, TIMEOUT_MILLIS);
    }

    @Override
    public CompletableFuture<Boolean> invoke(Condition condition, Operation success, Operation failure) {
        return invoke(condition, List.of(success), List.of(failure));
    }

    @Override
    public CompletableFuture<Boolean> invoke(
            Condition condition,
            List<Operation> success,
            List<Operation> failure
    ) {
        InvokeCommand invokeCommand = context.commandsFactory().invokeCommand()
                .condition(condition)
                .success(success)
                .failure(failure)
                .initiatorTime(clock.now())
                .id(commandIdGenerator.newId())
                .build();

        return context.raftService().run(invokeCommand, TIMEOUT_MILLIS);
    }

    @Override
    public CompletableFuture<StatementResult> invoke(Iif iif) {
        MultiInvokeCommand multiInvokeCommand = context.commandsFactory().multiInvokeCommand()
                .iif(iif)
                .initiatorTime(clock.now())
                .id(commandIdGenerator.newId())
                .build();

        return context.raftService().run(multiInvokeCommand, TIMEOUT_MILLIS);
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
        return range(keyFrom, keyTo, MetaStorageManager.LATEST_REVISION, includeTombstones);
    }

    @Override
    public Publisher<Entry> range(
            ByteArray keyFrom,
            @Nullable ByteArray keyTo,
            long revUpperBound,
            boolean includeTombstones
    ) {
        Function<byte[], ReadCommand> getRangeCommand = prevKey -> context.commandsFactory().getRangeCommand()
                .keyFrom(ByteBuffer.wrap(keyFrom.bytes()))
                .keyTo(keyTo == null ? null : ByteBuffer.wrap(keyTo.bytes()))
                .revUpperBound(revUpperBound)
                .includeTombstones(includeTombstones)
                .previousKey(prevKey)
                .batchSize(BATCH_SIZE)
                .build();

        return new CursorPublisher(context, getRangeCommand);
    }

    @Override
    public Publisher<Entry> prefix(ByteArray prefix, long revUpperBound) {
        Function<byte[], ReadCommand> getPrefixCommand = prevKey -> context.commandsFactory().getPrefixCommand()
                .prefix(ByteBuffer.wrap(prefix.bytes()))
                .revUpperBound(revUpperBound)
                .includeTombstones(false)
                .previousKey(prevKey)
                .batchSize(BATCH_SIZE)
                .build();

        return new CursorPublisher(context, getPrefixCommand);
    }

    /**
     * Sends idle safe time sync message. Should be called only on the leader node.
     *
     * @param safeTime New safe time.
     * @return Future that will be completed when message is sent.
     */
    CompletableFuture<Void> syncTime(HybridTimestamp safeTime, long term) {
        SyncTimeCommand syncTimeCommand = context.commandsFactory().syncTimeCommand()
                .initiatorTime(safeTime)
                .initiatorTerm(term)
                .build();

        return context.raftService().run(syncTimeCommand, TIMEOUT_MILLIS);
    }

    @Override
    public CompletableFuture<RevisionsInfo> currentRevisions() {
        GetCurrentRevisionsCommand cmd = context.commandsFactory().getCurrentRevisionsCommand().build();

        return context.raftService().run(cmd, RaftCommandRunner.NO_TIMEOUT);
    }

    @Override
    public CompletableFuture<ChecksumInfo> checksum(long revision) {
        GetChecksumCommand cmd = context.commandsFactory().getChecksumCommand()
                .revision(revision)
                .build();

        return context.raftService().run(cmd);
    }

    /**
     * Removes obsolete entries from both volatile and persistent idempotent command cache.
     *
     * @param evictionTimestamp Cached entries older than given timestamp will be evicted.
     * @return Pending operation future.
     */
    CompletableFuture<Void> evictIdempotentCommandsCache(HybridTimestamp evictionTimestamp) {
        EvictIdempotentCommandsCacheCommand evictIdempotentCommandsCacheCommand = evictIdempotentCommandsCacheCommand(
                context.commandsFactory(),
                evictionTimestamp,
                clock.now()
        );

        return context.raftService().run(evictIdempotentCommandsCacheCommand, TIMEOUT_MILLIS);
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

    /**
     * Creates put all command.
     *
     * @param commandsFactory Commands factory.
     * @param vals The map of keys and corresponding values. Couldn't be {@code null} or empty.
     * @param ts Local time.
     */
    private PutAllCommand putAllCommand(MetaStorageCommandsFactory commandsFactory, Map<ByteArray, byte[]> vals, HybridTimestamp ts) {
        assert !vals.isEmpty();

        int size = vals.size();

        var keys = new ArrayList<ByteBuffer>(size);
        var values = new ArrayList<ByteBuffer>(size);

        for (Map.Entry<ByteArray, byte[]> e : vals.entrySet()) {
            byte[] key = e.getKey().bytes();
            byte[] val = e.getValue();

            assert key != null : "Key could not be null.";
            assert val != null : "Value could not be null.";

            keys.add(ByteBuffer.wrap(key));
            values.add(ByteBuffer.wrap(val));
        }

        return commandsFactory.putAllCommand()
                .keys(keys)
                .values(values)
                .initiatorTime(ts)
                .build();
    }

    /**
     * Creates remove all command.
     *
     * @param commandsFactory Commands factory.
     * @param keys The keys collection. Couldn't be {@code null}.
     * @param ts Local time.
     */
    private RemoveAllCommand removeAllCommand(MetaStorageCommandsFactory commandsFactory, Set<ByteArray> keys, HybridTimestamp ts) {
        var list = new ArrayList<ByteBuffer>(keys.size());

        for (ByteArray key : keys) {
            list.add(ByteBuffer.wrap(key.bytes()));
        }

        return commandsFactory.removeAllCommand().keys(list).initiatorTime(ts).build();
    }

    /**
     * Creates evict idempotent commands cache command.
     *
     * @param commandsFactory Commands factory.
     * @param evictionTimestamp Cached entries older than given timestamp will be evicted.
     * @param ts Local time.
     */
    private EvictIdempotentCommandsCacheCommand evictIdempotentCommandsCacheCommand(
            MetaStorageCommandsFactory commandsFactory,
            HybridTimestamp evictionTimestamp,
            HybridTimestamp ts
    ) {
        return commandsFactory.evictIdempotentCommandsCacheCommand().evictionTimestamp(evictionTimestamp).initiatorTime(ts).build();
    }

    /**
     * Sends command {@link CompactionCommand} to the leader.
     *
     * @param compactionRevision New metastorage compaction revision.
     * @return Operation future.
     */
    CompletableFuture<Void> sendCompactionCommand(long compactionRevision) {
        CompactionCommand command = context.commandsFactory().compactionCommand()
                .compactionRevision(compactionRevision)
                .initiatorTime(clock.now())
                .build();

        return context.raftService().run(command, TIMEOUT_MILLIS);
    }

    // TODO: https://issues.apache.org/jira/browse/IGNITE-26085 Remove, tmp hack
    /**
     * Mark component as stopping.
     */
    void markAsStopping() {
        context.raftService().markAsStopping();
    }
}
