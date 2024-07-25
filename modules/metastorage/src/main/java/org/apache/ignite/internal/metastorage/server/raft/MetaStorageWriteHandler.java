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

import static java.util.Arrays.copyOfRange;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.util.ByteUtils.byteToBoolean;
import static org.apache.ignite.internal.util.ByteUtils.toByteArray;
import static org.apache.ignite.internal.util.ByteUtils.toByteArrayList;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import org.apache.ignite.configuration.ConfigurationValue;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.CommandId;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.command.IdempotentCommand;
import org.apache.ignite.internal.metastorage.command.InvokeCommand;
import org.apache.ignite.internal.metastorage.command.MetaStorageWriteCommand;
import org.apache.ignite.internal.metastorage.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.command.PutAllCommand;
import org.apache.ignite.internal.metastorage.command.PutCommand;
import org.apache.ignite.internal.metastorage.command.RemoveAllCommand;
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
import org.apache.ignite.internal.metastorage.server.OrCondition;
import org.apache.ignite.internal.metastorage.server.RevisionCondition;
import org.apache.ignite.internal.metastorage.server.Statement;
import org.apache.ignite.internal.metastorage.server.TombstoneCondition;
import org.apache.ignite.internal.metastorage.server.ValueCondition;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Class containing some common logic for Meta Storage Raft group listeners.
 */
public class MetaStorageWriteHandler {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(MetaStorageWriteHandler.class);

    public static final String IDEMPOTENT_COMMAND_PREFIX = "icp.";

    public static final byte[] IDEMPOTENT_COMMAND_PREFIX_BYTES = IDEMPOTENT_COMMAND_PREFIX.getBytes(StandardCharsets.UTF_8);

    private static final MetaStorageMessagesFactory MSG_FACTORY = new MetaStorageMessagesFactory();

    private final KeyValueStorage storage;
    private final ClusterTimeImpl clusterTime;

    private final Map<CommandId, IdempotentCommandCachedResult> idempotentCommandCache = new ConcurrentHashMap<>();

    private final ConfigurationValue<Long> idempotentCacheTtl;

    private final CompletableFuture<LongSupplier> maxClockSkewMillisFuture;

    MetaStorageWriteHandler(
            KeyValueStorage storage,
            ClusterTimeImpl clusterTime,
            ConfigurationValue<Long> idempotentCacheTtl,
            CompletableFuture<LongSupplier> maxClockSkewMillisFuture
    ) {
        this.storage = storage;
        this.clusterTime = clusterTime;
        this.idempotentCacheTtl = idempotentCacheTtl;
        this.maxClockSkewMillisFuture = maxClockSkewMillisFuture;
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

            IdempotentCommandCachedResult cachedResult = idempotentCommandCache.get(commandId);

            if (cachedResult != null) {
                clo.result(cachedResult.result);

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

        try {
            if (command instanceof MetaStorageWriteCommand) {
                var cmdWithTime = (MetaStorageWriteCommand) command;

                if (command instanceof SyncTimeCommand) {
                    var syncTimeCommand = (SyncTimeCommand) command;

                    // Ignore the command if it has been sent by a stale leader.
                    if (clo.term() != syncTimeCommand.initiatorTerm()) {
                        clo.result(null);

                        return;
                    }
                }

                handleWriteWithTime(clo, cmdWithTime);
            } else {
                assert false : "Command was not found [cmd=" + command + ']';
            }
        } catch (IgniteInternalException e) {
            clo.result(e);
        } catch (CompletionException e) {
            clo.result(e.getCause());
        } catch (Throwable t) {
            LOG.error(
                    "Unknown error while processing command [commandIndex={}, commandTerm={}, command={}]",
                    t,
                    clo.index(), clo.index(), command
            );

            throw t;
        }
    }

    /**
     * Handles {@link MetaStorageWriteCommand} command.
     *
     * @param clo Command closure.
     * @param command Command.
     */
    private void handleWriteWithTime(CommandClosure<WriteCommand> clo, MetaStorageWriteCommand command) {
        HybridTimestamp opTime = command.safeTime();

        if (command instanceof PutCommand) {
            PutCommand putCmd = (PutCommand) command;

            storage.put(toByteArray(putCmd.key()), toByteArray(putCmd.value()), opTime);

            clo.result(null);
        } else if (command instanceof PutAllCommand) {
            PutAllCommand putAllCmd = (PutAllCommand) command;

            storage.putAll(toByteArrayList(putAllCmd.keys()), toByteArrayList(putAllCmd.values()), opTime);

            clo.result(null);
        } else if (command instanceof RemoveCommand) {
            RemoveCommand rmvCmd = (RemoveCommand) command;

            storage.remove(toByteArray(rmvCmd.key()), opTime);

            clo.result(null);
        } else if (command instanceof RemoveAllCommand) {
            RemoveAllCommand rmvAllCmd = (RemoveAllCommand) command;

            storage.removeAll(toByteArrayList(rmvAllCmd.keys()), opTime);

            clo.result(null);
        } else if (command instanceof InvokeCommand) {
            InvokeCommand cmd = (InvokeCommand) command;

            clo.result(storage.invoke(toCondition(cmd.condition()), cmd.success(), cmd.failure(), opTime, cmd.id()));
        } else if (command instanceof MultiInvokeCommand) {
            MultiInvokeCommand cmd = (MultiInvokeCommand) command;

            clo.result(storage.invoke(toIf(cmd.iif()), opTime, cmd.id()));
        } else if (command instanceof SyncTimeCommand) {
            storage.advanceSafeTime(command.safeTime());

            clo.result(null);
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

    boolean beforeApply(Command command) {
        if (command instanceof MetaStorageWriteCommand) {
            // Initiator sends us a timestamp to adjust to.
            // Alter command by setting safe time based on the adjusted clock.
            MetaStorageWriteCommand writeCommand = (MetaStorageWriteCommand) command;

            clusterTime.adjust(writeCommand.initiatorTime());

            writeCommand.safeTime(clusterTime.now());

            return true;
        }

        return false;
    }

    /**
     * The callback that is called right after storage is updated with a snapshot.
     */
    void onSnapshotLoad() {
        byte[] keyFrom = IDEMPOTENT_COMMAND_PREFIX_BYTES;
        byte[] keyTo = storage.nextKey(IDEMPOTENT_COMMAND_PREFIX_BYTES);

        Cursor<Entry> cursor = storage.range(keyFrom, keyTo);
        // It's fine to lose original command start time - in that case we will store the entry a little bit longer that necessary.
        HybridTimestamp now = clusterTime.now();

        try (cursor) {
            for (Entry entry : cursor) {
                if (!entry.tombstone()) {
                    CommandId commandId = CommandId.fromString(
                            ByteUtils.stringFromBytes(entry.key()).substring(IDEMPOTENT_COMMAND_PREFIX.length(), entry.key().length));

                    Serializable result;
                    if (entry.value().length == 1) {
                        result = byteToBoolean(entry.value()[0]);
                    } else {
                        result = MSG_FACTORY.statementResult().result(ByteBuffer.wrap(entry.value())).build();
                    }

                    idempotentCommandCache.put(commandId, new IdempotentCommandCachedResult(result, now));
                }
            }
        }
    }

    /**
     * Removes obsolete entries from both volatile and persistent idempotent command cache.
     */
    // TODO: https://issues.apache.org/jira/browse/IGNITE-19417 Call on meta storage compaction.
    void evictIdempotentCommandsCache() {
        HybridTimestamp cleanupTimestamp = clusterTime.now();
        LOG.info("Idempotent command cache cleanup started [cleanupTimestamp={}].", cleanupTimestamp);

        maxClockSkewMillisFuture.thenAccept(maxClockSkewMillis -> {
            List<CommandId> commandIdsToRemove = idempotentCommandCache.entrySet().stream()
                    .filter(entry -> entry.getValue().commandStartTime.getPhysical()
                            <= cleanupTimestamp.getPhysical() - (idempotentCacheTtl.value() + maxClockSkewMillis.getAsLong()))
                    .map(Map.Entry::getKey)
                    .collect(toList());

            if (!commandIdsToRemove.isEmpty()) {
                List<byte[]> commandIdStorageKeys = commandIdsToRemove.stream()
                        .map(commandId -> ByteUtils.stringToBytes(IDEMPOTENT_COMMAND_PREFIX + commandId.toMGKeyAsString()))
                        .collect(toList());

                // TODO https://issues.apache.org/jira/browse/IGNITE-22819 Formally using clusterTime.now() as local operation timestamp is
                // TODO incorrect. It's not possible to use null, because RocksDB may throw AssertionException, thus locally triggered
                // TODO processing should be reworked with linearized leader based one.
                storage.removeAll(commandIdStorageKeys, clusterTime.now());

                commandIdsToRemove.forEach(idempotentCommandCache.keySet()::remove);
            }

            LOG.info("Idempotent command cache cleanup finished [cleanupTimestamp={}, cleanupCompletionTimestamp={},"
                            + " removedEntriesCount={}, cacheSize={}].",
                    cleanupTimestamp,
                    clusterTime.now(),
                    commandIdsToRemove.size(),
                    idempotentCommandCache.size()
            );
        });
    }

    private static class IdempotentCommandCachedResult {
        @Nullable
        final Serializable result;

        final HybridTimestamp commandStartTime;

        IdempotentCommandCachedResult(@Nullable Serializable result, HybridTimestamp commandStartTime) {
            this.result = result;
            this.commandStartTime = commandStartTime;
        }
    }

    private class ResultCachingClosure implements CommandClosure<WriteCommand> {
        CommandClosure<WriteCommand> closure;

        ResultCachingClosure(CommandClosure<WriteCommand> closure) {
            this.closure = closure;

            assert closure.command() instanceof IdempotentCommand;
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
                idempotentCommandCache.put(command.id(), new IdempotentCommandCachedResult(res, command.initiatorTime()));
            }

            closure.result(res);
        }
    }
}
