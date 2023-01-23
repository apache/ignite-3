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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.command.GetAndPutAllCommand;
import org.apache.ignite.internal.metastorage.command.GetAndPutCommand;
import org.apache.ignite.internal.metastorage.command.GetAndRemoveAllCommand;
import org.apache.ignite.internal.metastorage.command.GetAndRemoveCommand;
import org.apache.ignite.internal.metastorage.command.InvokeCommand;
import org.apache.ignite.internal.metastorage.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.metastorage.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.command.MultipleEntryResponse;
import org.apache.ignite.internal.metastorage.command.PutAllCommand;
import org.apache.ignite.internal.metastorage.command.PutCommand;
import org.apache.ignite.internal.metastorage.command.RemoveAllCommand;
import org.apache.ignite.internal.metastorage.command.RemoveCommand;
import org.apache.ignite.internal.metastorage.command.SingleEntryResponse;
import org.apache.ignite.internal.metastorage.command.info.CompoundConditionInfo;
import org.apache.ignite.internal.metastorage.command.info.ConditionInfo;
import org.apache.ignite.internal.metastorage.command.info.IfInfo;
import org.apache.ignite.internal.metastorage.command.info.OperationInfo;
import org.apache.ignite.internal.metastorage.command.info.SimpleConditionInfo;
import org.apache.ignite.internal.metastorage.command.info.StatementInfo;
import org.apache.ignite.internal.metastorage.command.info.UpdateInfo;
import org.apache.ignite.internal.metastorage.dsl.CompoundConditionType;
import org.apache.ignite.internal.metastorage.dsl.ConditionType;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.dsl.Update;
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
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;

/**
 * Class containing some common logic for Meta Storage Raft group listeners.
 */
class MetaStorageWriteHandler {
    private final MetaStorageCommandsFactory commandsFactory = new MetaStorageCommandsFactory();

    private final KeyValueStorage storage;

    MetaStorageWriteHandler(KeyValueStorage storage) {
        this.storage = storage;
    }

    /**
     * Tries to process a {@link WriteCommand}, returning {@code true} if the command has been successfully processed or {@code false}
     * if the command requires external processing.
     */
    boolean handleWriteCommand(CommandClosure<WriteCommand> clo) {
        WriteCommand command = clo.command();

        if (command instanceof PutCommand) {
            PutCommand putCmd = (PutCommand) command;

            storage.put(putCmd.key(), putCmd.value());

            clo.result(null);
        } else if (command instanceof GetAndPutCommand) {
            GetAndPutCommand getAndPutCmd = (GetAndPutCommand) command;

            Entry e = storage.getAndPut(getAndPutCmd.key(), getAndPutCmd.value());

            clo.result(new SingleEntryResponse(e.key(), e.value(), e.revision(), e.updateCounter()));
        } else if (command instanceof PutAllCommand) {
            PutAllCommand putAllCmd = (PutAllCommand) command;

            storage.putAll(putAllCmd.keys(), putAllCmd.values());

            clo.result(null);
        } else if (command instanceof GetAndPutAllCommand) {
            GetAndPutAllCommand getAndPutAllCmd = (GetAndPutAllCommand) command;

            Collection<Entry> entries = storage.getAndPutAll(getAndPutAllCmd.keys(), getAndPutAllCmd.values());

            List<SingleEntryResponse> resp = new ArrayList<>(entries.size());

            for (Entry e : entries) {
                resp.add(new SingleEntryResponse(e.key(), e.value(), e.revision(), e.updateCounter()));
            }

            clo.result(new MultipleEntryResponse(resp));
        } else if (command instanceof RemoveCommand) {
            RemoveCommand rmvCmd = (RemoveCommand) command;

            storage.remove(rmvCmd.key());

            clo.result(null);
        } else if (command instanceof GetAndRemoveCommand) {
            GetAndRemoveCommand getAndRmvCmd = (GetAndRemoveCommand) command;

            Entry e = storage.getAndRemove(getAndRmvCmd.key());

            clo.result(new SingleEntryResponse(e.key(), e.value(), e.revision(), e.updateCounter()));
        } else if (command instanceof RemoveAllCommand) {
            RemoveAllCommand rmvAllCmd = (RemoveAllCommand) command;

            storage.removeAll(rmvAllCmd.keys());

            clo.result(null);
        } else if (command instanceof GetAndRemoveAllCommand) {
            GetAndRemoveAllCommand getAndRmvAllCmd = (GetAndRemoveAllCommand) command;

            Collection<Entry> entries = storage.getAndRemoveAll(getAndRmvAllCmd.keys());

            List<SingleEntryResponse> resp = new ArrayList<>(entries.size());

            for (Entry e : entries) {
                resp.add(new SingleEntryResponse(e.key(), e.value(), e.revision(), e.updateCounter()));
            }

            clo.result(new MultipleEntryResponse(resp));
        } else if (command instanceof InvokeCommand) {
            InvokeCommand cmd = (InvokeCommand) command;

            boolean res = storage.invoke(
                    toCondition(cmd.condition()),
                    toOperations(cmd.success()),
                    toOperations(cmd.failure())
            );

            clo.result(res);
        } else if (command instanceof MultiInvokeCommand) {
            MultiInvokeCommand cmd = (MultiInvokeCommand) command;

            StatementResult res = storage.invoke(toIf(cmd.iif()));

            clo.result(commandsFactory.statementResultInfo().result(res.bytes()).build());
        } else {
            return false;
        }

        return true;
    }

    private static If toIf(IfInfo iif) {
        return new If(toCondition(iif.cond()), toConditionBranch(iif.andThen()), toConditionBranch(iif.orElse()));
    }

    private static Update toUpdate(UpdateInfo updateInfo) {
        return new Update(toOperations(new ArrayList<>(updateInfo.operations())), new StatementResult(updateInfo.result().result()));
    }

    private static Statement toConditionBranch(StatementInfo statementInfo) {
        if (statementInfo.isTerminal()) {
            return new Statement(toUpdate(statementInfo.update()));
        } else {
            return new Statement(toIf(statementInfo.iif()));
        }
    }

    private static Condition toCondition(ConditionInfo info) {
        if (info instanceof SimpleConditionInfo) {
            SimpleConditionInfo inf = (SimpleConditionInfo) info;
            byte[] key = inf.key();

            ConditionType type = inf.type();

            if (type == ConditionType.KEY_EXISTS) {
                return new ExistenceCondition(ExistenceCondition.Type.EXISTS, key);
            } else if (type == ConditionType.KEY_NOT_EXISTS) {
                return new ExistenceCondition(ExistenceCondition.Type.NOT_EXISTS, key);
            } else if (type == ConditionType.TOMBSTONE) {
                return new TombstoneCondition(key);
            } else if (type == ConditionType.VAL_EQUAL) {
                return new ValueCondition(ValueCondition.Type.EQUAL, key, inf.value());
            } else if (type == ConditionType.VAL_NOT_EQUAL) {
                return new ValueCondition(ValueCondition.Type.NOT_EQUAL, key, inf.value());
            } else if (type == ConditionType.VAL_GREATER) {
                return new ValueCondition(ValueCondition.Type.GREATER, key, inf.value());
            } else if (type == ConditionType.VAL_GREATER_OR_EQUAL) {
                return new ValueCondition(ValueCondition.Type.GREATER_OR_EQUAL, key, inf.value());
            } else if (type == ConditionType.VAL_LESS) {
                return new ValueCondition(ValueCondition.Type.LESS, key, inf.value());
            } else if (type == ConditionType.VAL_LESS_OR_EQUAL) {
                return new ValueCondition(ValueCondition.Type.LESS_OR_EQUAL, key, inf.value());
            } else if (type == ConditionType.REV_EQUAL) {
                return new RevisionCondition(RevisionCondition.Type.EQUAL, key, inf.revision());
            } else if (type == ConditionType.REV_NOT_EQUAL) {
                return new RevisionCondition(RevisionCondition.Type.NOT_EQUAL, key, inf.revision());
            } else if (type == ConditionType.REV_GREATER) {
                return new RevisionCondition(RevisionCondition.Type.GREATER, key, inf.revision());
            } else if (type == ConditionType.REV_GREATER_OR_EQUAL) {
                return new RevisionCondition(RevisionCondition.Type.GREATER_OR_EQUAL, key, inf.revision());
            } else if (type == ConditionType.REV_LESS) {
                return new RevisionCondition(RevisionCondition.Type.LESS, key, inf.revision());
            } else if (type == ConditionType.REV_LESS_OR_EQUAL) {
                return new RevisionCondition(RevisionCondition.Type.LESS_OR_EQUAL, key, inf.revision());
            } else {
                throw new IllegalArgumentException("Unknown condition type: " + type);
            }
        } else if (info instanceof CompoundConditionInfo) {
            CompoundConditionInfo inf = (CompoundConditionInfo) info;

            if (inf.type() == CompoundConditionType.AND) {
                return new AndCondition(toCondition(inf.leftConditionInfo()), toCondition(inf.rightConditionInfo()));

            } else if (inf.type() == CompoundConditionType.OR) {
                return new OrCondition(toCondition(inf.leftConditionInfo()), toCondition(inf.rightConditionInfo()));
            } else {
                throw new IllegalArgumentException("Unknown compound condition " + inf.type());
            }
        } else {
            throw new IllegalArgumentException("Unknown condition info type " + info);
        }
    }

    private static List<Operation> toOperations(List<OperationInfo> infos) {
        List<Operation> ops = new ArrayList<>(infos.size());

        for (OperationInfo info : infos) {
            ops.add(new Operation(info.type(), info.key(), info.value()));
        }

        return ops;
    }
}
