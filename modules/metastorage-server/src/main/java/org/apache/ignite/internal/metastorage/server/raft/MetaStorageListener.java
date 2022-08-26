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

package org.apache.ignite.internal.metastorage.server.raft;

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.lang.ErrorGroups.MetaStorage.CLOSING_STORAGE_ERR;
import static org.apache.ignite.lang.ErrorGroups.MetaStorage.CURSOR_CLOSING_ERR;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.ignite.internal.metastorage.common.ConditionType;
import org.apache.ignite.internal.metastorage.common.MetaStorageException;
import org.apache.ignite.internal.metastorage.common.StatementInfo;
import org.apache.ignite.internal.metastorage.common.StatementResultInfo;
import org.apache.ignite.internal.metastorage.common.UpdateInfo;
import org.apache.ignite.internal.metastorage.common.command.CompoundConditionInfo;
import org.apache.ignite.internal.metastorage.common.command.CompoundConditionType;
import org.apache.ignite.internal.metastorage.common.command.ConditionInfo;
import org.apache.ignite.internal.metastorage.common.command.GetAllCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndPutAllCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndPutCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndRemoveAllCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndRemoveCommand;
import org.apache.ignite.internal.metastorage.common.command.GetCommand;
import org.apache.ignite.internal.metastorage.common.command.IfInfo;
import org.apache.ignite.internal.metastorage.common.command.InvokeCommand;
import org.apache.ignite.internal.metastorage.common.command.MultiInvokeCommand;
import org.apache.ignite.internal.metastorage.common.command.MultipleEntryResponse;
import org.apache.ignite.internal.metastorage.common.command.OperationInfo;
import org.apache.ignite.internal.metastorage.common.command.PutAllCommand;
import org.apache.ignite.internal.metastorage.common.command.PutCommand;
import org.apache.ignite.internal.metastorage.common.command.RangeCommand;
import org.apache.ignite.internal.metastorage.common.command.RemoveAllCommand;
import org.apache.ignite.internal.metastorage.common.command.RemoveCommand;
import org.apache.ignite.internal.metastorage.common.command.SimpleConditionInfo;
import org.apache.ignite.internal.metastorage.common.command.SingleEntryResponse;
import org.apache.ignite.internal.metastorage.common.command.WatchExactKeysCommand;
import org.apache.ignite.internal.metastorage.common.command.WatchRangeKeysCommand;
import org.apache.ignite.internal.metastorage.common.command.cursor.CursorCloseCommand;
import org.apache.ignite.internal.metastorage.common.command.cursor.CursorHasNextCommand;
import org.apache.ignite.internal.metastorage.common.command.cursor.CursorNextCommand;
import org.apache.ignite.internal.metastorage.common.command.cursor.CursorsCloseCommand;
import org.apache.ignite.internal.metastorage.server.AndCondition;
import org.apache.ignite.internal.metastorage.server.Condition;
import org.apache.ignite.internal.metastorage.server.Entry;
import org.apache.ignite.internal.metastorage.server.EntryEvent;
import org.apache.ignite.internal.metastorage.server.ExistenceCondition;
import org.apache.ignite.internal.metastorage.server.If;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.Operation;
import org.apache.ignite.internal.metastorage.server.OrCondition;
import org.apache.ignite.internal.metastorage.server.RevisionCondition;
import org.apache.ignite.internal.metastorage.server.Statement;
import org.apache.ignite.internal.metastorage.server.StatementResult;
import org.apache.ignite.internal.metastorage.server.TombstoneCondition;
import org.apache.ignite.internal.metastorage.server.Update;
import org.apache.ignite.internal.metastorage.server.ValueCondition;
import org.apache.ignite.internal.metastorage.server.WatchEvent;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Meta storage listener.
 * TODO: IGNITE-14693 Implement Meta storage exception handling logic.
 */
public class MetaStorageListener implements RaftGroupListener {
    /** Storage. */
    private final KeyValueStorage storage;

    /** Cursors map. */
    private final Map<IgniteUuid, CursorMeta> cursors;

    /**
     * Constructor.
     *
     * @param storage Storage.
     */
    public MetaStorageListener(KeyValueStorage storage) {
        this.storage = storage;
        this.cursors = new ConcurrentHashMap<>();
    }

    /** {@inheritDoc} */
    @Override
    public void onRead(Iterator<CommandClosure<ReadCommand>> iter) {
        while (iter.hasNext()) {
            CommandClosure<ReadCommand> clo = iter.next();

            ReadCommand command = clo.command();

            if (command instanceof GetCommand) {
                GetCommand getCmd = (GetCommand) command;

                Entry e;

                if (getCmd.revision() != 0) {
                    e = storage.get(getCmd.key(), getCmd.revision());
                } else {
                    e = storage.get(getCmd.key());
                }

                SingleEntryResponse resp = new SingleEntryResponse(
                        e.key(), e.value(), e.revision(), e.updateCounter()
                );

                clo.result(resp);
            } else if (command instanceof GetAllCommand) {
                GetAllCommand getAllCmd = (GetAllCommand) command;

                Collection<Entry> entries;

                if (getAllCmd.revision() != 0) {
                    entries = storage.getAll(getAllCmd.keys(), getAllCmd.revision());
                } else {
                    entries = storage.getAll(getAllCmd.keys());
                }

                List<SingleEntryResponse> res = new ArrayList<>(entries.size());

                for (Entry e : entries) {
                    res.add(new SingleEntryResponse(e.key(), e.value(), e.revision(), e.updateCounter()));
                }

                clo.result(new MultipleEntryResponse(res));
            } else if (command instanceof CursorHasNextCommand) {
                CursorHasNextCommand cursorHasNextCmd = (CursorHasNextCommand) command;

                CursorMeta cursorDesc = cursors.get(cursorHasNextCmd.cursorId());

                clo.result(!(cursorDesc == null) && cursorDesc.cursor().hasNext());
            } else {
                assert false : "Command was not found [cmd=" + command + ']';
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onWrite(Iterator<CommandClosure<WriteCommand>> iter) {
        while (iter.hasNext()) {
            CommandClosure<WriteCommand> clo = iter.next();

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

                Collection<Entry> entries = storage.getAndPutAll(getAndPutAllCmd.keys(), getAndPutAllCmd.vals());

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

                clo.result(new StatementResultInfo(res.bytes()));
            } else if (command instanceof RangeCommand) {
                RangeCommand rangeCmd = (RangeCommand) command;

                IgniteUuid cursorId = rangeCmd.getCursorId();

                Cursor<Entry> cursor = (rangeCmd.revUpperBound() != -1)
                        ? storage.range(rangeCmd.keyFrom(), rangeCmd.keyTo(), rangeCmd.revUpperBound(), rangeCmd.includeTombstones()) :
                        storage.range(rangeCmd.keyFrom(), rangeCmd.keyTo(), rangeCmd.includeTombstones());

                cursors.put(
                        cursorId,
                        new CursorMeta(
                                cursor,
                                CursorType.RANGE,
                                rangeCmd.requesterNodeId(),
                                rangeCmd.batchSize()
                        )
                );

                clo.result(cursorId);
            } else if (command instanceof CursorNextCommand) {
                CursorNextCommand cursorNextCmd = (CursorNextCommand) command;

                CursorMeta cursorDesc = cursors.get(cursorNextCmd.cursorId());

                if (cursorDesc == null) {
                    clo.result(new NoSuchElementException("Corresponding cursor on the server side is not found."));

                    return;
                }

                try {
                    if (cursorDesc.type() == CursorType.RANGE) {
                        int batchSize = requireNonNull(cursorDesc.batchSize());

                        List<SingleEntryResponse> resp = new ArrayList<>(batchSize);

                        for (int i = 0; i < batchSize; i++) {
                            if (cursorDesc.cursor().hasNext()) {
                                Entry e = (Entry) cursorDesc.cursor().next();

                                resp.add(new SingleEntryResponse(e.key(), e.value(), e.revision(), e.updateCounter()));
                            } else {
                                break;
                            }
                        }

                        clo.result(new MultipleEntryResponse(resp));
                    } else if (cursorDesc.type() == CursorType.WATCH) {
                        WatchEvent evt = (WatchEvent) cursorDesc.cursor().next();

                        List<SingleEntryResponse> resp = new ArrayList<>(evt.entryEvents().size() * 2);

                        for (EntryEvent e : evt.entryEvents()) {
                            Entry o = e.oldEntry();

                            Entry n = e.entry();

                            resp.add(new SingleEntryResponse(o.key(), o.value(), o.revision(), o.updateCounter()));

                            resp.add(new SingleEntryResponse(n.key(), n.value(), n.revision(), n.updateCounter()));
                        }

                        clo.result(new MultipleEntryResponse(resp));
                    }
                } catch (NoSuchElementException e) {
                    clo.result(e);
                }
            } else if (command instanceof CursorCloseCommand) {
                CursorCloseCommand cursorCloseCmd = (CursorCloseCommand) command;

                CursorMeta cursorDesc = cursors.remove(cursorCloseCmd.cursorId());

                if (cursorDesc == null) {
                    clo.result(null);

                    return;
                }

                try {
                    cursorDesc.cursor().close();
                } catch (Exception e) {
                    throw new MetaStorageException(CURSOR_CLOSING_ERR, e);
                }

                clo.result(null);
            } else if (command instanceof WatchRangeKeysCommand) {
                WatchRangeKeysCommand watchCmd = (WatchRangeKeysCommand) command;

                IgniteUuid cursorId = watchCmd.getCursorId();

                Cursor<WatchEvent> cursor =
                        storage.watch(watchCmd.keyFrom(), watchCmd.keyTo(), watchCmd.revision());

                cursors.put(
                        cursorId,
                        new CursorMeta(
                                cursor,
                                CursorType.WATCH,
                                watchCmd.requesterNodeId(),
                                null
                        )
                );

                clo.result(cursorId);
            } else if (command instanceof WatchExactKeysCommand) {
                WatchExactKeysCommand watchCmd = (WatchExactKeysCommand) command;

                IgniteUuid cursorId = watchCmd.getCursorId();

                Cursor<WatchEvent> cursor = storage.watch(watchCmd.keys(), watchCmd.revision());

                cursors.put(
                        cursorId,
                        new CursorMeta(
                                cursor,
                                CursorType.WATCH,
                                watchCmd.requesterNodeId(),
                                null
                        )
                );

                clo.result(cursorId);
            } else if (command instanceof CursorsCloseCommand) {
                CursorsCloseCommand cursorsCloseCmd = (CursorsCloseCommand) command;

                Iterator<CursorMeta> cursorsIter = cursors.values().iterator();

                while (cursorsIter.hasNext()) {
                    CursorMeta cursorDesc = cursorsIter.next();

                    if (cursorDesc.requesterNodeId().equals(cursorsCloseCmd.nodeId())) {
                        try {
                            cursorDesc.cursor().close();
                        } catch (Exception e) {
                            throw new MetaStorageException(CURSOR_CLOSING_ERR, e);
                        }

                        cursorsIter.remove();
                    }

                }

                clo.result(null);
            } else {
                assert false : "Command was not found [cmd=" + command + ']';
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        storage.snapshot(path).whenComplete((unused, throwable) -> {
            doneClo.accept(throwable);
        });
    }

    /** {@inheritDoc} */
    @Override
    public boolean onSnapshotLoad(Path path) {
        storage.restoreSnapshot(path);
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public void onShutdown() {
        try {
            storage.close();
        } catch (Exception e) {
            throw new MetaStorageException(CLOSING_STORAGE_ERR, "Failed to close storage: " + e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public @Nullable CompletableFuture<Void> onBeforeApply(Command command) {
        return null;
    }

    /**
     * Returns {@link KeyValueStorage} that is backing this listener.
     */
    @TestOnly
    public KeyValueStorage getStorage() {
        return storage;
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

    /**
     * Cursor meta information: origin node id and type.
     */
    private class CursorMeta {
        /** Cursor. */
        private final Cursor<?> cursor;

        /** Cursor type. */
        private final CursorType type;

        /** Id of the node that creates cursor. */
        private final String requesterNodeId;

        /** Maximum size of the batch that is sent in single response message. */
        private final @Nullable Integer batchSize;

        /**
         * The constructor.
         *
         * @param cursor          Cursor.
         * @param type            Cursor type.
         * @param requesterNodeId Id of the node that creates cursor.
         * @param batchSize       Batch size.
         */
        CursorMeta(Cursor<?> cursor,
                CursorType type,
                String requesterNodeId,
                @Nullable Integer batchSize
        ) {
            this.cursor = cursor;
            this.type = type;
            this.requesterNodeId = requesterNodeId;
            this.batchSize = batchSize;
        }

        /**
         * Returns cursor.
         */
        public Cursor<?> cursor() {
            return cursor;
        }

        /**
         * Returns cursor type.
         */
        public CursorType type() {
            return type;
        }

        /**
         * Returns id of the node that creates cursor.
         */
        public String requesterNodeId() {
            return requesterNodeId;
        }

        /**
         * Returns maximum size of the batch that is sent in single response message.
         */
        public @Nullable Integer batchSize() {
            return batchSize;
        }
    }

    /** Cursor type. */
    private enum CursorType {
        RANGE,

        WATCH;
    }
}
