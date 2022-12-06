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

package org.apache.ignite.internal.metastorage.client;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.metastorage.common.command.GetAllCommand.getAllCommand;
import static org.apache.ignite.internal.metastorage.common.command.GetAndPutAllCommand.getAndPutAllCommand;
import static org.apache.ignite.internal.metastorage.common.command.GetAndRemoveAllCommand.getAndRemoveAllCommand;
import static org.apache.ignite.internal.metastorage.common.command.PutAllCommand.putAllCommand;
import static org.apache.ignite.internal.metastorage.common.command.RemoveAllCommand.removeAllCommand;
import static org.apache.ignite.internal.metastorage.common.command.WatchExactKeysCommand.watchExactKeysCommand;
import static org.apache.ignite.lang.ErrorGroups.MetaStorage.WATCH_STOPPING_ERR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.common.MetaStorageException;
import org.apache.ignite.internal.metastorage.common.OperationInfo;
import org.apache.ignite.internal.metastorage.common.OperationInfoBuilder;
import org.apache.ignite.internal.metastorage.common.OperationType;
import org.apache.ignite.internal.metastorage.common.StatementInfo;
import org.apache.ignite.internal.metastorage.common.StatementResultInfo;
import org.apache.ignite.internal.metastorage.common.UpdateInfo;
import org.apache.ignite.internal.metastorage.common.command.ConditionInfo;
import org.apache.ignite.internal.metastorage.common.command.GetAllCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndPutAllCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndPutCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndRemoveAllCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndRemoveCommand;
import org.apache.ignite.internal.metastorage.common.command.GetCommand;
import org.apache.ignite.internal.metastorage.common.command.IfInfo;
import org.apache.ignite.internal.metastorage.common.command.InvokeCommand;
import org.apache.ignite.internal.metastorage.common.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.metastorage.common.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.common.command.MultipleEntryResponse;
import org.apache.ignite.internal.metastorage.common.command.PutAllCommand;
import org.apache.ignite.internal.metastorage.common.command.PutCommand;
import org.apache.ignite.internal.metastorage.common.command.RangeCommand;
import org.apache.ignite.internal.metastorage.common.command.RemoveAllCommand;
import org.apache.ignite.internal.metastorage.common.command.RemoveCommand;
import org.apache.ignite.internal.metastorage.common.command.SimpleConditionInfoBuilder;
import org.apache.ignite.internal.metastorage.common.command.SingleEntryResponse;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.IgniteUuidGenerator;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link MetaStorageService} implementation.
 */
public class MetaStorageServiceImpl implements MetaStorageService {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(MetaStorageServiceImpl.class);

    /** IgniteUuid generator. */
    private static final IgniteUuidGenerator uuidGenerator = new IgniteUuidGenerator(UUID.randomUUID(), 0);

    /** Commands factory. */
    private final MetaStorageCommandsFactory commandsFactory = new MetaStorageCommandsFactory();

    /** Meta storage raft group service. */
    private final RaftGroupService metaStorageRaftGrpSvc;

    // TODO: IGNITE-14691 Temporally solution that should be removed after implementing reactive watches.
    /** Watch processor, that uses pulling logic in order to retrieve watch notifications from server. */
    private final WatchProcessor watchProcessor;

    /** Local node id. */
    private final String localNodeId;

    /** Local node name. */
    private final String localNodeName;

    /**
     * Constructor.
     *
     * @param metaStorageRaftGrpSvc Meta storage raft group service.
     * @param localNodeId           Local node id.
     * @param localNodeName         Local node name.
     */
    public MetaStorageServiceImpl(RaftGroupService metaStorageRaftGrpSvc, String localNodeId, String localNodeName) {
        this.metaStorageRaftGrpSvc = metaStorageRaftGrpSvc;
        this.watchProcessor = new WatchProcessor();
        this.localNodeId = localNodeId;
        this.localNodeName = localNodeName;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Entry> get(@NotNull ByteArray key) {
        GetCommand getCommand = commandsFactory.getCommand().key(key.bytes()).build();

        return metaStorageRaftGrpSvc.run(getCommand).thenApply(MetaStorageServiceImpl::singleEntryResult);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Entry> get(@NotNull ByteArray key, long revUpperBound) {
        GetCommand getCommand = commandsFactory.getCommand().key(key.bytes()).revision(revUpperBound).build();

        return metaStorageRaftGrpSvc.run(getCommand).thenApply(MetaStorageServiceImpl::singleEntryResult);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys) {
        GetAllCommand getAllCommand = getAllCommand(commandsFactory, keys, 0);

        return metaStorageRaftGrpSvc.run(getAllCommand).thenApply(MetaStorageServiceImpl::multipleEntryResult);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys, long revUpperBound) {
        GetAllCommand getAllCommand = getAllCommand(commandsFactory, keys, revUpperBound);

        return metaStorageRaftGrpSvc.run(getAllCommand).thenApply(MetaStorageServiceImpl::multipleEntryResult);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> put(ByteArray key, byte[] value) {
        PutCommand putCommand = commandsFactory.putCommand().key(key.bytes()).value(value).build();

        return metaStorageRaftGrpSvc.run(putCommand);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Entry> getAndPut(ByteArray key, byte[] value) {
        GetAndPutCommand getAndPutCommand = commandsFactory.getAndPutCommand().key(key.bytes()).value(value).build();

        return metaStorageRaftGrpSvc.run(getAndPutCommand).thenApply(MetaStorageServiceImpl::singleEntryResult);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> putAll(@NotNull Map<ByteArray, byte[]> vals) {
        PutAllCommand putAllCommand = putAllCommand(commandsFactory, vals);

        return metaStorageRaftGrpSvc.run(putAllCommand);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAndPutAll(@NotNull Map<ByteArray, byte[]> vals) {
        GetAndPutAllCommand getAndPutAllCommand = getAndPutAllCommand(commandsFactory, vals);

        return metaStorageRaftGrpSvc.run(getAndPutAllCommand).thenApply(MetaStorageServiceImpl::multipleEntryResult);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> remove(@NotNull ByteArray key) {
        RemoveCommand removeCommand = commandsFactory.removeCommand().key(key.bytes()).build();

        return metaStorageRaftGrpSvc.run(removeCommand);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Entry> getAndRemove(@NotNull ByteArray key) {
        GetAndRemoveCommand getAndRemoveCommand = commandsFactory.getAndRemoveCommand().key(key.bytes()).build();

        return metaStorageRaftGrpSvc.run(getAndRemoveCommand).thenApply(MetaStorageServiceImpl::singleEntryResult);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> removeAll(@NotNull Set<ByteArray> keys) {
        RemoveAllCommand removeAllCommand = removeAllCommand(commandsFactory, keys);

        return metaStorageRaftGrpSvc.run(removeAllCommand);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAndRemoveAll(@NotNull Set<ByteArray> keys) {
        GetAndRemoveAllCommand getAndRemoveAllCommand = getAndRemoveAllCommand(commandsFactory, keys);

        return metaStorageRaftGrpSvc.run(getAndRemoveAllCommand).thenApply(MetaStorageServiceImpl::multipleEntryResult);
    }

    @Override
    public @NotNull CompletableFuture<Boolean> invoke(
            @NotNull Condition condition,
            @NotNull Operation success,
            @NotNull Operation failure
    ) {
        return invoke(condition, List.of(success), List.of(failure));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> invoke(
            @NotNull Condition condition,
            @NotNull Collection<Operation> success,
            @NotNull Collection<Operation> failure
    ) {
        ConditionInfo cond = toConditionInfo(condition, commandsFactory);

        List<OperationInfo> successOps = toOperationInfos(success, commandsFactory);

        List<OperationInfo> failureOps = toOperationInfos(failure, commandsFactory);

        InvokeCommand invokeCommand = commandsFactory.invokeCommand().condition(cond).success(successOps).failure(failureOps).build();

        return metaStorageRaftGrpSvc.run(invokeCommand);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<StatementResult> invoke(@NotNull If iif) {
        MultiInvokeCommand multiInvokeCommand = commandsFactory.multiInvokeCommand().iif(toIfInfo(iif, commandsFactory)).build();

        return metaStorageRaftGrpSvc.run(multiInvokeCommand)
                .thenApply(bi -> new StatementResult(((StatementResultInfo) bi).result()));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull Cursor<Entry> range(@NotNull ByteArray keyFrom, @Nullable ByteArray keyTo, long revUpperBound) {
        return range(keyFrom, keyTo, revUpperBound, false);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull Cursor<Entry> range(
            @NotNull ByteArray keyFrom,
            @Nullable ByteArray keyTo,
            long revUpperBound,
            boolean includeTombstones
    ) {
        return new CursorImpl<>(
                commandsFactory,
                metaStorageRaftGrpSvc,
                metaStorageRaftGrpSvc.run(
                        commandsFactory.rangeCommand()
                                .keyFrom(keyFrom.bytes())
                                .keyTo(keyTo == null ? null : keyTo.bytes())
                                .requesterNodeId(localNodeId)
                                .cursorId(uuidGenerator.randomUuid())
                                .revUpperBound(revUpperBound)
                                .includeTombstones(includeTombstones)
                                .batchSize(RangeCommand.DEFAULT_BATCH_SIZE)
                                .build()
                ),
                MetaStorageServiceImpl::multipleEntryResultForCache
        );
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull Cursor<Entry> range(@NotNull ByteArray keyFrom, @Nullable ByteArray keyTo) {
        return range(keyFrom, keyTo, false);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull Cursor<Entry> range(@NotNull ByteArray keyFrom, @Nullable ByteArray keyTo, boolean includeTombstones) {
        return new CursorImpl<>(
                commandsFactory,
            metaStorageRaftGrpSvc,
            metaStorageRaftGrpSvc.run(
                    commandsFactory.rangeCommand()
                            .keyFrom(keyFrom.bytes())
                            .keyTo(keyTo == null ? null : keyTo.bytes())
                            .revUpperBound(-1)
                            .requesterNodeId(localNodeId)
                            .cursorId(uuidGenerator.randomUuid())
                            .includeTombstones(includeTombstones)
                            .batchSize(RangeCommand.DEFAULT_BATCH_SIZE)
                            .build()
            ),
            MetaStorageServiceImpl::multipleEntryResultForCache
        );
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<IgniteUuid> watch(
            @Nullable ByteArray keyFrom,
            @Nullable ByteArray keyTo,
            long revision,
            @NotNull WatchListener lsnr
    ) {
        CompletableFuture<IgniteUuid> watchRes = metaStorageRaftGrpSvc.run(commandsFactory.watchRangeKeysCommand()
                .keyFrom(keyFrom == null ? null : keyFrom.bytes())
                .keyTo(keyTo == null ? null : keyTo.bytes())
                .revision(revision)
                .requesterNodeId(localNodeId)
                .cursorId(uuidGenerator.randomUuid())
                .build()
        );

        watchRes.thenAccept(
                watchId -> watchProcessor.addWatch(
                        watchId,
                        new CursorImpl<>(commandsFactory, metaStorageRaftGrpSvc, watchRes, MetaStorageServiceImpl::watchResponse),
                        lsnr
                )
        );

        return watchRes;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<IgniteUuid> watch(
            @NotNull ByteArray key,
            long revision,
            @NotNull WatchListener lsnr
    ) {
        return watch(key, null, revision, lsnr);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<IgniteUuid> watch(
            @NotNull Set<ByteArray> keys,
            long revision,
            @NotNull WatchListener lsnr
    ) {
        CompletableFuture<IgniteUuid> watchRes =
                metaStorageRaftGrpSvc.run(watchExactKeysCommand(commandsFactory, keys, revision, localNodeId, uuidGenerator.randomUuid()));

        watchRes.thenAccept(
                watchId -> watchProcessor.addWatch(
                        watchId,
                        new CursorImpl<>(commandsFactory, metaStorageRaftGrpSvc, watchRes, MetaStorageServiceImpl::watchResponse),
                        lsnr
                )
        );

        return watchRes;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> stopWatch(@NotNull IgniteUuid id) {
        return CompletableFuture.runAsync(() -> watchProcessor.stopWatch(id));
    }

    // TODO: IGNITE-14734 Implement.

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> compact() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> closeCursors(@NotNull String nodeId) {
        return metaStorageRaftGrpSvc.run(commandsFactory.cursorsCloseCommand().nodeId(nodeId).build());
    }

    public static List<OperationInfo> toOperationInfos(Collection<Operation> ops, MetaStorageCommandsFactory commandsFactory) {
        List<OperationInfo> res = new ArrayList<>(ops.size());

        for (Operation op : ops) {
            OperationInfoBuilder info = commandsFactory.operationInfo();

            switch (op.type()) {
                case NO_OP:
                    info.operationType(OperationType.NO_OP.ordinal());

                    break;

                case REMOVE:
                    info.key(op.inner().key()).operationType(OperationType.REMOVE.ordinal());

                    break;

                case PUT:
                    Operation.PutOp inner = (Operation.PutOp) op.inner();

                    info.key(inner.key()).value(inner.value()).operationType(OperationType.PUT.ordinal());

                    break;

                default:
                    assert false : "Unknown operation type " + op.type();
            }

            res.add(info.build());
        }

        return res;
    }

    private static UpdateInfo toUpdateInfo(Update update, MetaStorageCommandsFactory commandsFactory) {
        return commandsFactory.updateInfo()
                .operations(toOperationInfos(update.operations(), commandsFactory))
                .result(commandsFactory.statementResultInfo().result(update.result().bytes()).build())
                .build();
    }

    private static StatementInfo toIfBranchInfo(Statement statement, MetaStorageCommandsFactory commandsFactory) {
        if (statement.isTerminal()) {
            return commandsFactory.statementInfo().update(toUpdateInfo(statement.update(), commandsFactory)).build();
        } else {
            return commandsFactory.statementInfo().iif(toIfInfo(statement.iif(), commandsFactory)).build();
        }
    }

    public static IfInfo toIfInfo(If iif, MetaStorageCommandsFactory commandsFactory) {
        return commandsFactory.ifInfo()
                .cond(toConditionInfo(iif.condition(), commandsFactory))
                .andThen(toIfBranchInfo(iif.andThen(), commandsFactory))
                .orElse(toIfBranchInfo(iif.orElse(), commandsFactory))
                .build();
    }

    private static ConditionInfo toConditionInfo(@NotNull Condition condition, MetaStorageCommandsFactory commandsFactory) {
        if (condition instanceof SimpleCondition) {
            SimpleConditionInfoBuilder cnd = commandsFactory.simpleConditionInfo();

            Object obj = ((SimpleCondition) condition).inner();

            if (obj instanceof SimpleCondition.ExistenceCondition) {
                SimpleCondition.ExistenceCondition inner = (SimpleCondition.ExistenceCondition) obj;

                cnd.key(inner.key()).conditionType(inner.type().ordinal());
            } else if (obj instanceof SimpleCondition.TombstoneCondition) {
                SimpleCondition.TombstoneCondition inner = (SimpleCondition.TombstoneCondition) obj;

                cnd.key(inner.key()).conditionType(inner.type().ordinal());
            } else if (obj instanceof SimpleCondition.RevisionCondition) {
                SimpleCondition.RevisionCondition inner = (SimpleCondition.RevisionCondition) obj;

                cnd.key(inner.key()).conditionType(inner.type().ordinal()).revision(inner.revision());
            } else if (obj instanceof SimpleCondition.ValueCondition) {
                SimpleCondition.ValueCondition inner = (SimpleCondition.ValueCondition) obj;

                cnd.key(inner.key()).conditionType(inner.type().ordinal()).value(inner.value());
            } else {
                assert false : "Unknown condition type: " + obj.getClass().getSimpleName();
            }

            return cnd.build();
        } else if (condition instanceof CompoundCondition) {
            CompoundCondition cond = (CompoundCondition) condition;

            return commandsFactory.compoundConditionInfo()
                    .leftConditionInfo(toConditionInfo(cond.leftCondition(), commandsFactory))
                    .rightConditionInfo(toConditionInfo(cond.rightCondition(), commandsFactory))
                    .conditionType(cond.compoundConditionType().ordinal())
                    .build();
        } else {
            assert false : "Unknown condition type: " + condition.getClass().getSimpleName();

            return null;
        }
    }

    private static Map<ByteArray, Entry> multipleEntryResult(Object obj) {
        MultipleEntryResponse resp = (MultipleEntryResponse) obj;

        Map<ByteArray, Entry> res = new HashMap<>();

        for (SingleEntryResponse e : resp.entries()) {
            ByteArray key = new ByteArray(e.key());

            res.put(key, new EntryImpl(key, e.value(), e.revision(), e.updateCounter()));
        }

        return res;
    }

    private static List<Entry> multipleEntryResultForCache(Object obj) {
        MultipleEntryResponse resp = (MultipleEntryResponse) obj;

        return resp.entries().stream()
            .map(MetaStorageServiceImpl::singleEntryResult)
            .collect(toList());
    }

    private static Entry singleEntryResult(Object obj) {
        SingleEntryResponse resp = (SingleEntryResponse) obj;

        return new EntryImpl(new ByteArray(resp.key()), resp.value(), resp.revision(), resp.updateCounter());
    }

    private static WatchEvent watchResponse(Object obj) {
        MultipleEntryResponse resp = (MultipleEntryResponse) obj;

        List<EntryEvent> evts = new ArrayList<>(resp.entries().size() / 2);

        Entry o = null;
        Entry n;

        for (int i = 0; i < resp.entries().size(); i++) {
            SingleEntryResponse s = resp.entries().get(i);

            EntryImpl e = new EntryImpl(new ByteArray(s.key()), s.value(), s.revision(), s.updateCounter());

            if (i % 2 == 0) {
                o = e;
            } else {
                n = e;

                evts.add(new EntryEvent(o, n));
            }
        }

        return new WatchEvent(evts);
    }

    // TODO: IGNITE-14691 Temporally solution that should be removed after implementing reactive watches.

    /** Watch processor, that manages {@link Watcher} threads. */
    private final class WatchProcessor {
        /** Active Watcher threads that process notification pulling logic. */
        private final Map<IgniteUuid, Watcher> watchers = new ConcurrentHashMap<>();

        /**
         * Starts exclusive thread per watch that implement watch pulling logic and calls {@link WatchListener#onUpdate(WatchEvent)}} or
         * {@link WatchListener#onError(Throwable)}.
         *
         * @param watchId Watch id.
         * @param cursor  Watch Cursor.
         * @param lsnr    The listener which receives and handles watch updates.
         */
        private void addWatch(IgniteUuid watchId, CursorImpl<WatchEvent> cursor, WatchListener lsnr) {
            Watcher watcher = new Watcher(cursor, lsnr);

            watchers.put(watchId, watcher);

            watcher.start();
        }

        /**
         * Closes server cursor and interrupts watch pulling thread.
         *
         * @param watchId Watch id.
         */
        private void stopWatch(IgniteUuid watchId) {
            watchers.computeIfPresent(
                    watchId,
                    (k, v) -> {
                        CompletableFuture.runAsync(() -> {
                            v.stop = true;

                            v.interrupt();
                        }).thenRun(() -> {
                            try {
                                Thread.sleep(100);

                                v.cursor.close();
                            } catch (InterruptedException e) {
                                throw new MetaStorageException(WATCH_STOPPING_ERR, e);
                            } catch (Exception e) {
                                if (e instanceof IgniteInternalException && e.getCause().getCause() instanceof RejectedExecutionException) {
                                    LOG.debug("Cursor close command was rejected because raft executor has been already stopped");
                                    return;
                                }

                                // TODO: IGNITE-14693 Implement Meta storage exception handling logic.
                                LOG.warn("Unexpected exception", e);
                            }
                        });
                        return null;
                    }
            );
        }

        /** Watcher thread, uses pulling logic in order to retrieve watch notifications from server. */
        private final class Watcher extends Thread {
            private volatile boolean stop = false;

            /** Watch event cursor. */
            private Cursor<WatchEvent> cursor;

            /** The listener which receives and handles watch updates. */
            private WatchListener lsnr;

            /**
             * Constructor.
             *
             * @param cursor Watch event cursor.
             * @param lsnr   The listener which receives and handles watch updates.
             */
            Watcher(Cursor<WatchEvent> cursor, WatchListener lsnr) {
                setName("ms-watcher-" + localNodeName);
                this.cursor = cursor;
                this.lsnr = lsnr;
            }

            /**
             * Pulls watch events from server side with the help of cursor.iterator.hasNext()/next() in the while(true) loop. Collects watch
             * events with same revision and fires either onUpdate or onError().
             */
            @Override
            public void run() {
                Iterator<WatchEvent> watchEvtsIter = cursor.iterator();

                while (!stop) {
                    try {
                        if (watchEvtsIter.hasNext()) {
                            WatchEvent watchEvt = null;

                            try {
                                watchEvt = watchEvtsIter.next();
                            } catch (Throwable e) {
                                lsnr.onError(e);

                                throw e;
                            }

                            assert watchEvt != null;

                            lsnr.onUpdate(watchEvt);
                        } else {
                            Thread.sleep(10);
                        }
                    } catch (Throwable e) {
                        if (e instanceof NodeStoppingException || e.getCause() instanceof NodeStoppingException) {
                            break;
                        } else if ((e instanceof InterruptedException || e.getCause() instanceof InterruptedException) && stop) {
                            LOG.debug("Watcher has been stopped during node's stop");

                            break;
                        } else {
                            // TODO: IGNITE-14693 Implement Meta storage exception handling logic.
                            LOG.warn("Unexpected exception", e);
                        }
                    }
                }
            }
        }
    }
}
