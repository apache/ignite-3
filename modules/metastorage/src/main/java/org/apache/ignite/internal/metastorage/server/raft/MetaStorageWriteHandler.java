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

package org.apache.ignite.internal.metastorage.server.raft;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorage.INVOKE_RESULT_FALSE_BYTES;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorage.INVOKE_RESULT_TRUE_BYTES;
import static org.apache.ignite.internal.util.ByteUtils.byteToBoolean;
import static org.apache.ignite.internal.util.ByteUtils.toByteArray;
import static org.apache.ignite.internal.util.ByteUtils.toByteArrayList;
import static org.apache.ignite.internal.util.StringUtils.toStringWithoutPrefix;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntConsumer;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.CommandId;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.command.CompactionCommand;
import org.apache.ignite.internal.metastorage.command.EvictIdempotentCommandsCacheCommand;
import org.apache.ignite.internal.metastorage.command.IdempotentCommand;
import org.apache.ignite.internal.metastorage.command.InvokeCommand;
import org.apache.ignite.internal.metastorage.command.MetaStorageWriteCommand;
import org.apache.ignite.internal.metastorage.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.command.PutAllCommand;
import org.apache.ignite.internal.metastorage.command.PutCommand;
import org.apache.ignite.internal.metastorage.command.RemoveAllCommand;
import org.apache.ignite.internal.metastorage.command.RemoveByPrefixCommand;
import org.apache.ignite.internal.metastorage.command.RemoveCommand;
import org.apache.ignite.internal.metastorage.command.SyncTimeCommand;
import org.apache.ignite.internal.metastorage.dsl.CompoundCondition;
import org.apache.ignite.internal.metastorage.dsl.ConditionType;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.MetaStorageMessagesFactory;
import org.apache.ignite.internal.metastorage.dsl.SimpleCondition;
import org.apache.ignite.internal.metastorage.dsl.Statement.IfStatement;
import org.apache.ignite.internal.metastorage.dsl.Statement.UpdateStatement;
import org.apache.ignite.internal.metastorage.server.AndCondition;
import org.apache.ignite.internal.metastorage.server.Condition;
import org.apache.ignite.internal.metastorage.server.ExistenceCondition;
import org.apache.ignite.internal.metastorage.server.If;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.KeyValueUpdateContext;
import org.apache.ignite.internal.metastorage.server.OrCondition;
import org.apache.ignite.internal.metastorage.server.RevisionCondition;
import org.apache.ignite.internal.metastorage.server.Statement;
import org.apache.ignite.internal.metastorage.server.TombstoneCondition;
import org.apache.ignite.internal.metastorage.server.ValueCondition;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Class containing some common logic for Meta Storage Raft group listeners.
 */
public class MetaStorageWriteHandler {
    private static final IgniteLogger LOG = Loggers.forClass(MetaStorageWriteHandler.class);

    public static final String IDEMPOTENT_COMMAND_PREFIX = "icp.";

    public static final byte[] IDEMPOTENT_COMMAND_PREFIX_BYTES = IDEMPOTENT_COMMAND_PREFIX.getBytes(StandardCharsets.UTF_8);

    private static final MetaStorageMessagesFactory MSG_FACTORY = new MetaStorageMessagesFactory();

    private final KeyValueStorage storage;
    private final HybridClock clock;
    private final ClusterTimeImpl clusterTime;

    private final IntConsumer idempotentCacheSizeListener;

    private final Map<CommandId, CommandResultAndTimestamp> idempotentCommandCache = new ConcurrentHashMap<>();

    MetaStorageWriteHandler(
            KeyValueStorage storage,
            HybridClock clock,
            ClusterTimeImpl clusterTime,
            IntConsumer idempotentCacheSizeListener
    ) {
        this.storage = storage;
        this.clock = clock;
        this.clusterTime = clusterTime;
        this.idempotentCacheSizeListener = idempotentCacheSizeListener;
    }

    /**
     * Processes a given {@link WriteCommand}.
     */
    void handleWriteCommand(CommandClosure<WriteCommand> clo) {
        WriteCommand command = clo.command();

        CommandClosure<WriteCommand> resultClosure;

        if (command instanceof IdempotentCommand) {
            IdempotentCommand idempotentCommand = ((IdempotentCommand) command);
            CommandId commandId = idempotentCommand.id();

            CommandResultAndTimestamp cachedResult = idempotentCommandCache.get(commandId);

            if (cachedResult != null) {
                Serializable commandResult = cachedResult.commandResult;

                // For MultiInvokeCommand, represent boolean results as 0/1 wrapped in a StatementResult,
                // because the closure for this command type expects a StatementResult.
                // Rationale: when restoring the idempotent-command cache from a snapshot, single-byte values (0/1)
                // are read as booleans, while StatementResult persists raw bytes (0 or 1) and exposes them via its API
                // (for example, getAsBoolean()).
                // This maintains backward compatibility.
                if (commandResult instanceof Boolean && command instanceof MultiInvokeCommand) {
                    var booleanResult = (Boolean) commandResult;

                    clo.result(MSG_FACTORY.statementResult()
                            .result(ByteBuffer.wrap(booleanResult ? INVOKE_RESULT_TRUE_BYTES : INVOKE_RESULT_FALSE_BYTES))
                            .build()
                    );
                } else {
                    clo.result(commandResult);
                }

                return;
            } else {
                resultClosure = new ResultCachingClosure(clo);
            }
        } else {
            resultClosure = clo;
        }

        handleNonCachedWriteCommand(resultClosure);
    }

    private void handleNonCachedWriteCommand(CommandClosure<WriteCommand> clo) {
        WriteCommand command = clo.command();

        long commandIndex = clo.index();
        long commandTerm = clo.term();

        try {
            if (command instanceof MetaStorageWriteCommand) {
                var cmdWithTime = (MetaStorageWriteCommand) command;

                if (command instanceof SyncTimeCommand) {
                    var syncTimeCommand = (SyncTimeCommand) command;

                    // Ignore the command if it has been sent by a stale leader.
                    if (commandTerm != syncTimeCommand.initiatorTerm()) {
                        LOG.info("Sync time command closure term {}, initiator term {}, ignoring the command",
                                commandTerm, syncTimeCommand.initiatorTerm()
                        );

                        storage.setIndexAndTerm(commandIndex, commandTerm);

                        clo.result(null);

                        return;
                    }
                }

                handleWriteWithTime(clo, cmdWithTime, commandIndex, commandTerm);
            } else {
                assert false : "Command was not found [cmd=" + command + ']';
            }
        } catch (Throwable e) {
            LOG.error(
                    "Unknown error while processing command [commandIndex={}, commandTerm={}, command={}]",
                    e,
                    commandIndex, commandTerm, command
            );

            clo.result(e);

            // Rethrowing to let JRaft know that the state machine might be broken.
            throw e;
        }
    }

    /**
     * Handles {@link MetaStorageWriteCommand} command.
     *
     * @param clo Command closure.
     * @param command Command.
     * @param index Command index.
     * @param term Command term.
     */
    private void handleWriteWithTime(CommandClosure<WriteCommand> clo, MetaStorageWriteCommand command, long index, long term) {
        HybridTimestamp opTime = command.safeTime();

        var context = new KeyValueUpdateContext(index, term, opTime);

        if (command instanceof PutCommand) {
            PutCommand putCmd = (PutCommand) command;

            storage.put(toByteArray(putCmd.key()), toByteArray(putCmd.value()), context);

            clo.result(null);
        } else if (command instanceof PutAllCommand) {
            PutAllCommand putAllCmd = (PutAllCommand) command;

            storage.putAll(toByteArrayList(putAllCmd.keys()), toByteArrayList(putAllCmd.values()), context);

            clo.result(null);
        } else if (command instanceof RemoveCommand) {
            RemoveCommand rmvCmd = (RemoveCommand) command;

            storage.remove(toByteArray(rmvCmd.key()), context);

            clo.result(null);
        } else if (command instanceof RemoveAllCommand) {
            RemoveAllCommand rmvAllCmd = (RemoveAllCommand) command;

            storage.removeAll(toByteArrayList(rmvAllCmd.keys()), context);

            clo.result(null);
        } else if (command instanceof RemoveByPrefixCommand) {
            RemoveByPrefixCommand rmvByPrefixCmd = (RemoveByPrefixCommand) command;

            storage.removeByPrefix(toByteArray((rmvByPrefixCmd.prefix())), context);

            clo.result(null);
        } else if (command instanceof InvokeCommand) {
            InvokeCommand cmd = (InvokeCommand) command;

            clo.result(storage.invoke(toCondition(cmd.condition()), cmd.success(), cmd.failure(), context, cmd.id()));
        } else if (command instanceof MultiInvokeCommand) {
            MultiInvokeCommand cmd = (MultiInvokeCommand) command;

            clo.result(storage.invoke(toIf(cmd.iif()), context, cmd.id()));
        } else if (command instanceof SyncTimeCommand) {
            storage.advanceSafeTime(context);

            clo.result(null);
        } else if (command instanceof EvictIdempotentCommandsCacheCommand) {
            EvictIdempotentCommandsCacheCommand cmd = (EvictIdempotentCommandsCacheCommand) command;
            evictIdempotentCommandsCache(cmd.evictionTimestamp(), context);

            clo.result(null);
        } else if (command instanceof CompactionCommand) {
            CompactionCommand cmd = (CompactionCommand) command;

            storage.updateCompactionRevision(cmd.compactionRevision(), context);

            clo.result(null);
        } else {
            throw new AssertionError(String.format("Unsupported command: [context=%s, command=%s]", context, command));
        }
    }

    public static If toIf(Iif iif) {
        return new If(toCondition(iif.condition()), toConditionBranch(iif.andThen()), toConditionBranch(iif.orElse()));
    }

    private static Statement toConditionBranch(org.apache.ignite.internal.metastorage.dsl.Statement statement) {
        if (statement instanceof IfStatement) {
            return new Statement(toIf(((IfStatement) statement).iif()));
        } else if (statement instanceof UpdateStatement) {
            return new Statement(((UpdateStatement) statement).update());
        } else {
            throw new IllegalArgumentException("Unexpected statement type: " + statement);
        }
    }

    private static Condition toCondition(org.apache.ignite.internal.metastorage.dsl.Condition condition) {
        if (condition instanceof SimpleCondition.ValueCondition) {
            var valueCondition = (SimpleCondition.ValueCondition) condition;

            return new ValueCondition(
                    toValueConditionType(valueCondition.type()),
                    toByteArray(valueCondition.key()),
                    toByteArray(valueCondition.value())
            );
        } else if (condition instanceof SimpleCondition.RevisionCondition) {
            var revisionCondition = (SimpleCondition.RevisionCondition) condition;

            return new RevisionCondition(
                    toRevisionConditionType(revisionCondition.type()),
                    toByteArray(revisionCondition.key()),
                    revisionCondition.revision()
            );
        } else if (condition instanceof SimpleCondition) {
            var simpleCondition = (SimpleCondition) condition;

            switch (simpleCondition.type()) {
                case KEY_EXISTS:
                    return new ExistenceCondition(ExistenceCondition.Type.EXISTS, toByteArray(simpleCondition.key()));

                case KEY_NOT_EXISTS:
                    return new ExistenceCondition(ExistenceCondition.Type.NOT_EXISTS, toByteArray(simpleCondition.key()));

                case TOMBSTONE:
                    return new TombstoneCondition(TombstoneCondition.Type.TOMBSTONE, toByteArray(simpleCondition.key()));

                case NOT_TOMBSTONE:
                    return new TombstoneCondition(TombstoneCondition.Type.NOT_TOMBSTONE, toByteArray(simpleCondition.key()));

                default:
                    throw new IllegalArgumentException("Unexpected simple condition type " + simpleCondition.type());
            }
        } else if (condition instanceof CompoundCondition) {
            CompoundCondition compoundCondition = (CompoundCondition) condition;

            Condition leftCondition = toCondition(compoundCondition.leftCondition());
            Condition rightCondition = toCondition(compoundCondition.rightCondition());

            switch (compoundCondition.type()) {
                case AND:
                    return new AndCondition(leftCondition, rightCondition);

                case OR:
                    return new OrCondition(leftCondition, rightCondition);

                default:
                    throw new IllegalArgumentException("Unexpected compound condition type " + compoundCondition.type());
            }
        } else {
            throw new IllegalArgumentException("Unknown condition " + condition);
        }
    }

    private static ValueCondition.Type toValueConditionType(ConditionType type) {
        switch (type) {
            case VAL_EQUAL:
                return ValueCondition.Type.EQUAL;
            case VAL_NOT_EQUAL:
                return ValueCondition.Type.NOT_EQUAL;
            case VAL_GREATER:
                return ValueCondition.Type.GREATER;
            case VAL_GREATER_OR_EQUAL:
                return ValueCondition.Type.GREATER_OR_EQUAL;
            case VAL_LESS:
                return ValueCondition.Type.LESS;
            case VAL_LESS_OR_EQUAL:
                return ValueCondition.Type.LESS_OR_EQUAL;
            default:
                throw new IllegalArgumentException("Unexpected value condition type " + type);
        }
    }

    private static RevisionCondition.Type toRevisionConditionType(ConditionType type) {
        switch (type) {
            case REV_EQUAL:
                return RevisionCondition.Type.EQUAL;
            case REV_NOT_EQUAL:
                return RevisionCondition.Type.NOT_EQUAL;
            case REV_GREATER:
                return RevisionCondition.Type.GREATER;
            case REV_GREATER_OR_EQUAL:
                return RevisionCondition.Type.GREATER_OR_EQUAL;
            case REV_LESS:
                return RevisionCondition.Type.LESS;
            case REV_LESS_OR_EQUAL:
                return RevisionCondition.Type.LESS_OR_EQUAL;
            default:
                throw new IllegalArgumentException("Unexpected revision condition type " + type);
        }
    }

    Command beforeApply(Command command) {
        if (command instanceof MetaStorageWriteCommand) {
            // Initiator sends us a timestamp to adjust to.
            // Alter command by setting safe time based on the adjusted clock.
            // We need to clone the original command, because we have no control over its lifecycle. For example, raft client might send it
            // second time during the retry, this would lead to changing an already processing command in such a case, which is a data race.
            MetaStorageWriteCommand writeCommand = (MetaStorageWriteCommand) command.clone();

            clusterTime.adjustClock(writeCommand.initiatorTime());

            writeCommand.safeTime(clock.now());

            return writeCommand;
        }

        return command;
    }

    /**
     * The callback that is called right after storage is updated with a snapshot.
     */
    void onSnapshotLoad() {
        byte[] keyFrom = IDEMPOTENT_COMMAND_PREFIX_BYTES;
        byte[] keyTo = storage.nextKey(IDEMPOTENT_COMMAND_PREFIX_BYTES);

        try (Cursor<Entry> cursor = storage.range(keyFrom, keyTo)) {
            for (Entry entry : cursor) {
                if (!entry.tombstone()) {
                    String commandIdString = toStringWithoutPrefix(entry.key(), IDEMPOTENT_COMMAND_PREFIX_BYTES.length);

                    CommandId commandId = CommandId.fromString(commandIdString);

                    Serializable result;

                    byte[] entryValue = entry.value();

                    assert entryValue != null;

                    // A single-byte array is not guaranteed to represent a boolean.
                    // This guard avoids treating arbitrary 1-byte values as booleans.
                    // We apply it uniformly (including for StatementResult) to keep backward compatibility.
                    // When reading from the cache, boolean results are normalized to 0/1 so a correct StatementResult can be rebuilt.
                    if (entryValue.length == 1 && (entryValue[0] | 1) == 1) {
                        result = byteToBoolean(entryValue[0]);
                    } else {
                        result = MSG_FACTORY.statementResult().result(ByteBuffer.wrap(entryValue)).build();
                    }

                    idempotentCommandCache.put(commandId, new CommandResultAndTimestamp(result, entry.timestamp()));
                }
            }

            idempotentCacheSizeListener.accept(idempotentCommandCache.size());
        }
    }

    /**
     * Removes obsolete entries from both volatile and persistent idempotent command cache.
     *
     * @param evictionTimestamp Cached entries older than given timestamp will be evicted.
     * @param context Command operation context.
     */
    private void evictIdempotentCommandsCache(HybridTimestamp evictionTimestamp, KeyValueUpdateContext context) {
        List<CommandId> evictedCommandIds = evictCommandsFromCache(evictionTimestamp);

        if (evictedCommandIds.isEmpty()) {
            return;
        }

        storage.removeAll(toIdempotentCommandKeyBytes(evictedCommandIds), context);
    }

    private class ResultCachingClosure implements CommandClosure<WriteCommand> {
        final CommandClosure<WriteCommand> closure;

        ResultCachingClosure(CommandClosure<WriteCommand> closure) {
            assert closure.command() instanceof IdempotentCommand;

            this.closure = closure;
        }

        @Override
        public long index() {
            return closure.index();
        }

        @Override
        public long term() {
            return closure.term();
        }

        @Override
        public WriteCommand command() {
            return closure.command();
        }

        @Override
        public void result(@Nullable Serializable res) {
            IdempotentCommand command = (IdempotentCommand) closure.command();

            // Exceptions are not cached.
            if (!(res instanceof Throwable)) {
                idempotentCommandCache.put(command.id(), new CommandResultAndTimestamp(res, command.safeTime()));

                idempotentCacheSizeListener.accept(idempotentCommandCache.size());
            }

            closure.result(res);
        }
    }

    private List<CommandId> evictCommandsFromCache(HybridTimestamp evictionTimestamp) {
        Iterator<Map.Entry<CommandId, CommandResultAndTimestamp>> iterator = idempotentCommandCache.entrySet().iterator();

        var result = new ArrayList<CommandId>();

        while (iterator.hasNext()) {
            Map.Entry<CommandId, CommandResultAndTimestamp> entry = iterator.next();

            if (evictionTimestamp.compareTo(entry.getValue().commandTimestamp) >= 0) {
                iterator.remove();

                result.add(entry.getKey());
            }
        }

        idempotentCacheSizeListener.accept(idempotentCommandCache.size());

        return result;
    }

    private static List<byte[]> toIdempotentCommandKeyBytes(List<CommandId> commandIds) {
        return commandIds.stream()
                .map(MetaStorageWriteHandler::toIdempotentCommandKey)
                .map(ByteArray::bytes)
                .collect(toList());
    }

    /** Converts to independent command key. */
    public static ByteArray toIdempotentCommandKey(CommandId commandId) {
        return new ByteArray(IDEMPOTENT_COMMAND_PREFIX + commandId.toMgKeyAsString());
    }
}
