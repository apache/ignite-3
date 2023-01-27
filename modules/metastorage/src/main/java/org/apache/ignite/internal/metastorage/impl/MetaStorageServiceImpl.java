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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.metastorage.command.GetAllCommand.getAllCommand;
import static org.apache.ignite.internal.metastorage.command.GetAndPutAllCommand.getAndPutAllCommand;
import static org.apache.ignite.internal.metastorage.command.GetAndRemoveAllCommand.getAndRemoveAllCommand;
import static org.apache.ignite.internal.metastorage.command.PutAllCommand.putAllCommand;
import static org.apache.ignite.internal.metastorage.command.RemoveAllCommand.removeAllCommand;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
import org.apache.ignite.internal.metastorage.command.MultipleEntryResponse;
import org.apache.ignite.internal.metastorage.command.PrefixCommand;
import org.apache.ignite.internal.metastorage.command.PutAllCommand;
import org.apache.ignite.internal.metastorage.command.PutCommand;
import org.apache.ignite.internal.metastorage.command.RangeCommand;
import org.apache.ignite.internal.metastorage.command.RemoveAllCommand;
import org.apache.ignite.internal.metastorage.command.RemoveCommand;
import org.apache.ignite.internal.metastorage.command.SingleEntryResponse;
import org.apache.ignite.internal.metastorage.command.info.ConditionInfo;
import org.apache.ignite.internal.metastorage.command.info.IfInfo;
import org.apache.ignite.internal.metastorage.command.info.OperationInfo;
import org.apache.ignite.internal.metastorage.command.info.OperationInfoBuilder;
import org.apache.ignite.internal.metastorage.command.info.SimpleConditionInfoBuilder;
import org.apache.ignite.internal.metastorage.command.info.StatementInfo;
import org.apache.ignite.internal.metastorage.command.info.StatementResultInfo;
import org.apache.ignite.internal.metastorage.command.info.UpdateInfo;
import org.apache.ignite.internal.metastorage.dsl.CompoundCondition;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.If;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.OperationType;
import org.apache.ignite.internal.metastorage.dsl.SimpleCondition;
import org.apache.ignite.internal.metastorage.dsl.SimpleCondition.RevisionCondition;
import org.apache.ignite.internal.metastorage.dsl.SimpleCondition.ValueCondition;
import org.apache.ignite.internal.metastorage.dsl.Statement;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.dsl.Update;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteUuidGenerator;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * {@link MetaStorageService} implementation.
 */
public class MetaStorageServiceImpl implements MetaStorageService {
    /** IgniteUuid generator. */
    private static final IgniteUuidGenerator uuidGenerator = new IgniteUuidGenerator(UUID.randomUUID(), 0);

    /** Commands factory. */
    private final MetaStorageCommandsFactory commandsFactory = new MetaStorageCommandsFactory();

    /** Meta storage raft group service. */
    private final RaftGroupService metaStorageRaftGrpSvc;

    /** Local node. */
    private final ClusterNode localNode;

    /**
     * Constructor.
     *
     * @param metaStorageRaftGrpSvc Meta storage raft group service.
     * @param localNode Local node.
     */
    public MetaStorageServiceImpl(RaftGroupService metaStorageRaftGrpSvc, ClusterNode localNode) {
        this.metaStorageRaftGrpSvc = metaStorageRaftGrpSvc;
        this.localNode = localNode;
    }

    RaftGroupService raftGroupService() {
        return metaStorageRaftGrpSvc;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Entry> get(ByteArray key) {
        GetCommand getCommand = commandsFactory.getCommand().key(key.bytes()).build();

        return metaStorageRaftGrpSvc.run(getCommand).thenApply(MetaStorageServiceImpl::singleEntryResult);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Entry> get(ByteArray key, long revUpperBound) {
        GetCommand getCommand = commandsFactory.getCommand().key(key.bytes()).revision(revUpperBound).build();

        return metaStorageRaftGrpSvc.run(getCommand).thenApply(MetaStorageServiceImpl::singleEntryResult);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys) {
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
    public CompletableFuture<Entry> getAndPut(ByteArray key, byte[] value) {
        GetAndPutCommand getAndPutCommand = commandsFactory.getAndPutCommand().key(key.bytes()).value(value).build();

        return metaStorageRaftGrpSvc.run(getAndPutCommand).thenApply(MetaStorageServiceImpl::singleEntryResult);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> putAll(Map<ByteArray, byte[]> vals) {
        PutAllCommand putAllCommand = putAllCommand(commandsFactory, vals);

        return metaStorageRaftGrpSvc.run(putAllCommand);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Map<ByteArray, Entry>> getAndPutAll(Map<ByteArray, byte[]> vals) {
        GetAndPutAllCommand getAndPutAllCommand = getAndPutAllCommand(commandsFactory, vals);

        return metaStorageRaftGrpSvc.run(getAndPutAllCommand).thenApply(MetaStorageServiceImpl::multipleEntryResult);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> remove(ByteArray key) {
        RemoveCommand removeCommand = commandsFactory.removeCommand().key(key.bytes()).build();

        return metaStorageRaftGrpSvc.run(removeCommand);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Entry> getAndRemove(ByteArray key) {
        GetAndRemoveCommand getAndRemoveCommand = commandsFactory.getAndRemoveCommand().key(key.bytes()).build();

        return metaStorageRaftGrpSvc.run(getAndRemoveCommand).thenApply(MetaStorageServiceImpl::singleEntryResult);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> removeAll(Set<ByteArray> keys) {
        RemoveAllCommand removeAllCommand = removeAllCommand(commandsFactory, keys);

        return metaStorageRaftGrpSvc.run(removeAllCommand);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Map<ByteArray, Entry>> getAndRemoveAll(Set<ByteArray> keys) {
        GetAndRemoveAllCommand getAndRemoveAllCommand = getAndRemoveAllCommand(commandsFactory, keys);

        return metaStorageRaftGrpSvc.run(getAndRemoveAllCommand).thenApply(MetaStorageServiceImpl::multipleEntryResult);
    }

    @Override
    public CompletableFuture<Boolean> invoke(
            Condition condition,
            Operation success,
            Operation failure
    ) {
        return invoke(condition, List.of(success), List.of(failure));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> invoke(
            Condition condition,
            Collection<Operation> success,
            Collection<Operation> failure
    ) {
        ConditionInfo cond = toConditionInfo(condition, commandsFactory);

        List<OperationInfo> successOps = toOperationInfos(success, commandsFactory);

        List<OperationInfo> failureOps = toOperationInfos(failure, commandsFactory);

        InvokeCommand invokeCommand = commandsFactory.invokeCommand().condition(cond).success(successOps).failure(failureOps).build();

        return metaStorageRaftGrpSvc.run(invokeCommand);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<StatementResult> invoke(If iif) {
        MultiInvokeCommand multiInvokeCommand = commandsFactory.multiInvokeCommand().iif(toIfInfo(iif, commandsFactory)).build();

        return metaStorageRaftGrpSvc.run(multiInvokeCommand)
                .thenApply(bi -> new StatementResult(((StatementResultInfo) bi).result()));
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo, long revUpperBound) {
        return range(keyFrom, keyTo, revUpperBound, false);
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<Entry> range(
            ByteArray keyFrom,
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
                                .requesterNodeId(localNode.id())
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
    public Cursor<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo) {
        return range(keyFrom, keyTo, false);
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<Entry> range(ByteArray keyFrom, @Nullable ByteArray keyTo, boolean includeTombstones) {
        return range(keyFrom, keyTo, -1, includeTombstones);
    }

    @Override
    public Cursor<Entry> prefix(ByteArray prefix, long revUpperBound) {
        return new CursorImpl<>(
                commandsFactory,
                metaStorageRaftGrpSvc,
                metaStorageRaftGrpSvc.run(
                        commandsFactory.prefixCommand()
                                .prefix(prefix.bytes())
                                .revUpperBound(revUpperBound)
                                .requesterNodeId(localNode.id())
                                .cursorId(uuidGenerator.randomUuid())
                                .includeTombstones(false)
                                .batchSize(PrefixCommand.DEFAULT_BATCH_SIZE)
                                .build()
                ),
                MetaStorageServiceImpl::multipleEntryResultForCache
        );
    }

    // TODO: IGNITE-14734 Implement.
    @Override
    public CompletableFuture<Void> compact() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> closeCursors(String nodeId) {
        return metaStorageRaftGrpSvc.run(commandsFactory.cursorsCloseCommand().nodeId(nodeId).build());
    }

    @Override
    public void close() {
        metaStorageRaftGrpSvc.shutdown();
    }

    private static List<OperationInfo> toOperationInfos(Collection<Operation> ops, MetaStorageCommandsFactory commandsFactory) {
        List<OperationInfo> res = new ArrayList<>(ops.size());

        for (Operation op : ops) {
            OperationInfoBuilder info = commandsFactory.operationInfo();

            switch (op.type()) {
                case NO_OP:
                    info.operationType(OperationType.NO_OP.ordinal());

                    break;

                case REMOVE:
                    info.key(op.key()).operationType(OperationType.REMOVE.ordinal());

                    break;

                case PUT:
                    info.key(op.key()).value(op.value()).operationType(OperationType.PUT.ordinal());

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

    /**
     * Generate network message from {@link If} statement.
     *
     * @param iif If statement.
     * @param commandsFactory Commands factory.
     * @return Network message.
     */
    public static IfInfo toIfInfo(If iif, MetaStorageCommandsFactory commandsFactory) {
        return commandsFactory.ifInfo()
                .cond(toConditionInfo(iif.condition(), commandsFactory))
                .andThen(toIfBranchInfo(iif.andThen(), commandsFactory))
                .orElse(toIfBranchInfo(iif.orElse(), commandsFactory))
                .build();
    }

    private static ConditionInfo toConditionInfo(Condition condition, MetaStorageCommandsFactory commandsFactory) {
        if (condition instanceof SimpleCondition) {
            var simpleCondition = (SimpleCondition) condition;

            SimpleConditionInfoBuilder cnd = commandsFactory.simpleConditionInfo()
                    .key(simpleCondition.key())
                    .conditionType(simpleCondition.type().ordinal());

            if (simpleCondition instanceof SimpleCondition.RevisionCondition) {
                cnd.revision(((RevisionCondition) simpleCondition).revision());
            } else if (simpleCondition instanceof SimpleCondition.ValueCondition) {
                cnd.value(((ValueCondition) simpleCondition).value());
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
            throw new IllegalArgumentException("Unknown condition type: " + condition.getClass().getSimpleName());
        }
    }

    private static Map<ByteArray, Entry> multipleEntryResult(Object obj) {
        MultipleEntryResponse resp = (MultipleEntryResponse) obj;

        Map<ByteArray, Entry> res = new HashMap<>();

        for (SingleEntryResponse e : resp.entries()) {
            res.put(new ByteArray(e.key()), new EntryImpl(e.key(), e.value(), e.revision(), e.updateCounter()));
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

        return new EntryImpl(resp.key(), resp.value(), resp.revision(), resp.updateCounter());
    }
}
