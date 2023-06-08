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

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.CompletionException;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.command.GetAndPutAllCommand;
import org.apache.ignite.internal.metastorage.command.GetAndPutCommand;
import org.apache.ignite.internal.metastorage.command.GetAndRemoveAllCommand;
import org.apache.ignite.internal.metastorage.command.GetAndRemoveCommand;
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
import org.apache.ignite.lang.IgniteInternalException;

/**
 * Class containing some common logic for Meta Storage Raft group listeners.
 */
class MetaStorageWriteHandler {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(MetaStorageWriteHandler.class);

    private final KeyValueStorage storage;
    private final ClusterTimeImpl clusterTime;

    MetaStorageWriteHandler(KeyValueStorage storage, ClusterTimeImpl clusterTime) {
        this.storage = storage;
        this.clusterTime = clusterTime;
    }

    /**
     * Processes a given {@link WriteCommand}.
     */
    void handleWriteCommand(CommandClosure<WriteCommand> clo) {
        WriteCommand command = clo.command();

        try {
            HybridTimestamp safeTime;

            if (command instanceof MetaStorageWriteCommand) {
                MetaStorageWriteCommand cmdWithTime = (MetaStorageWriteCommand) command;

                safeTime = cmdWithTime.safeTime();

                handleWriteWithTime(clo, cmdWithTime, safeTime);
            } else if (command instanceof SyncTimeCommand) {
                // TODO: IGNITE-19199 WatchProcessor must be notified of the new safe time.
                throw new UnsupportedOperationException("https://issues.apache.org/jira/browse/IGNITE-19199");
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
     * @param opTime Command's time.
     */
    private void handleWriteWithTime(CommandClosure<WriteCommand> clo, MetaStorageWriteCommand command, HybridTimestamp opTime) {
        if (command instanceof PutCommand) {
            PutCommand putCmd = (PutCommand) command;

            storage.put(putCmd.key(), putCmd.value(), opTime);

            clo.result(null);
        } else if (command instanceof GetAndPutCommand) {
            GetAndPutCommand getAndPutCmd = (GetAndPutCommand) command;

            Entry e = storage.getAndPut(getAndPutCmd.key(), getAndPutCmd.value(), opTime);

            clo.result(e);
        } else if (command instanceof PutAllCommand) {
            PutAllCommand putAllCmd = (PutAllCommand) command;

            storage.putAll(putAllCmd.keys(), putAllCmd.values(), opTime);

            clo.result(null);
        } else if (command instanceof GetAndPutAllCommand) {
            GetAndPutAllCommand getAndPutAllCmd = (GetAndPutAllCommand) command;

            Collection<Entry> entries = storage.getAndPutAll(getAndPutAllCmd.keys(), getAndPutAllCmd.values(), opTime);

            clo.result((Serializable) entries);
        } else if (command instanceof RemoveCommand) {
            RemoveCommand rmvCmd = (RemoveCommand) command;

            storage.remove(rmvCmd.key(), opTime);

            clo.result(null);
        } else if (command instanceof GetAndRemoveCommand) {
            GetAndRemoveCommand getAndRmvCmd = (GetAndRemoveCommand) command;

            Entry e = storage.getAndRemove(getAndRmvCmd.key(), opTime);

            clo.result(e);
        } else if (command instanceof RemoveAllCommand) {
            RemoveAllCommand rmvAllCmd = (RemoveAllCommand) command;

            storage.removeAll(rmvAllCmd.keys(), opTime);

            clo.result(null);
        } else if (command instanceof GetAndRemoveAllCommand) {
            GetAndRemoveAllCommand getAndRmvAllCmd = (GetAndRemoveAllCommand) command;

            Collection<Entry> entries = storage.getAndRemoveAll(getAndRmvAllCmd.keys(), opTime);

            clo.result((Serializable) entries);
        } else if (command instanceof InvokeCommand) {
            InvokeCommand cmd = (InvokeCommand) command;

            clo.result(storage.invoke(toCondition(cmd.condition()), cmd.success(), cmd.failure(), opTime));
        } else if (command instanceof MultiInvokeCommand) {
            MultiInvokeCommand cmd = (MultiInvokeCommand) command;

            clo.result(storage.invoke(toIf(cmd.iif()), opTime));
        }
    }

    private static If toIf(Iif iif) {
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
                    valueCondition.key(),
                    valueCondition.value()
            );
        } else if (condition instanceof SimpleCondition.RevisionCondition) {
            var revisionCondition = (SimpleCondition.RevisionCondition) condition;

            return new RevisionCondition(
                    toRevisionConditionType(revisionCondition.type()),
                    revisionCondition.key(),
                    revisionCondition.revision()
            );
        } else if (condition instanceof SimpleCondition) {
            var simpleCondition = (SimpleCondition) condition;

            switch (simpleCondition.type()) {
                case KEY_EXISTS:
                    return new ExistenceCondition(ExistenceCondition.Type.EXISTS, simpleCondition.key());

                case KEY_NOT_EXISTS:
                    return new ExistenceCondition(ExistenceCondition.Type.NOT_EXISTS, simpleCondition.key());

                case TOMBSTONE:
                    return new TombstoneCondition(simpleCondition.key());

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

    void beforeApply(Command command) {
        if (command instanceof MetaStorageWriteCommand) {
            // Initiator sends us a timestamp to adjust to.
            // Alter command by setting safe time based on the adjusted clock.
            MetaStorageWriteCommand writeCommand = (MetaStorageWriteCommand) command;

            clusterTime.adjust(writeCommand.initiatorTime());

            writeCommand.safeTimeLong(clusterTime.nowLong());
        }
    }
}
