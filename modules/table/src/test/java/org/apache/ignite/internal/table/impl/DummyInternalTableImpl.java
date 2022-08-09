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

package org.apache.ignite.internal.table.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import javax.naming.OperationNotSupportedException;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.storage.basic.TestMvPartitionStorage;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.distributed.command.GetAllCommand;
import org.apache.ignite.internal.table.distributed.command.GetCommand;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.table.distributed.storage.VersionedRowStore;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mockito.Mockito;

/**
 * Dummy table storage implementation.
 */
public class DummyInternalTableImpl extends InternalTableImpl {
    public static final NetworkAddress ADDR = new NetworkAddress("127.0.0.1", 2004);

    private PartitionListener partitionListener;

    /**
     * Creates a new local table.
     *
     * @param txManager Transaction manager.
     */
    public DummyInternalTableImpl(TxManager txManager, AtomicLong raftIndex) {
        super("test", UUID.randomUUID(),
                Int2ObjectMaps.singleton(0, mock(RaftGroupService.class)),
                1, null, null, txManager, mock(MvTableStorage.class), mock(ReplicaService.class));

        RaftGroupService svc = partitionMap.get(0);

        Mockito.doReturn("testGrp").when(svc).groupId();
        Mockito.doReturn(new Peer(ADDR)).when(svc).leader();

        // Delegate directly to listener.
        doAnswer(
                invocationClose -> {
                    Command cmd = invocationClose.getArgument(0);

                    long commandIndex = raftIndex.incrementAndGet();

                    CompletableFuture<Serializable> res = new CompletableFuture<>();

                    CompletableFuture<Void> fut = partitionListener.onBeforeApply(cmd);

                    fut.handle(new BiFunction<Void, Throwable, Void>() {
                        @Override
                        public Void apply(Void ignored, Throwable err) {
                            if (err == null) {
                                if (cmd instanceof GetCommand || cmd instanceof GetAllCommand) {
                                    CommandClosure<ReadCommand> clo = new CommandClosure<>() {
                                        @Override
                                        public ReadCommand command() {
                                            return (ReadCommand) cmd;
                                        }

                                        @Override
                                        public void result(@Nullable Serializable r) {
                                            res.complete(r);
                                        }
                                    };

                                    try {
                                        partitionListener.onRead(List.of(clo).iterator());
                                    } catch (Throwable e) {
                                        res.completeExceptionally(new TransactionException(e));
                                    }
                                } else {
                                    CommandClosure<WriteCommand> clo = new CommandClosure<>() {
                                        /** {@inheritDoc} */
                                        @Override
                                        public long index() {
                                            return commandIndex;
                                        }

                                        /** {@inheritDoc} */
                                        @Override
                                        public WriteCommand command() {
                                            return (WriteCommand) cmd;
                                        }

                                        /** {@inheritDoc} */
                                        @Override
                                        public void result(@Nullable Serializable r) {
                                            res.complete(r);
                                        }
                                    };

                                    try {
                                        partitionListener.onWrite(List.of(clo).iterator());
                                    } catch (Throwable e) {
                                        res.completeExceptionally(new TransactionException(e));
                                    }
                                }
                            } else {
                                res.completeExceptionally(err);
                            }

                            return null;
                        }
                    });

                    return res;
                }
        ).when(svc).run(any());

        UUID tblId = UUID.randomUUID();

        partitionListener = new PartitionListener(
                tblId,
                new VersionedRowStore(new TestMvPartitionStorage(List.of(), 0), txManager)
        );
    }

    /**
     * Partition listener.
     *
     * @return Partition listener.
     */
    public PartitionListener getPartitionListener() {
        return partitionListener;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull UUID tableId() {
        return UUID.randomUUID();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String name() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<BinaryRow> get(BinaryRowEx keyRow, InternalTransaction tx) {
        return super.get(keyRow, tx);
    }

    /** {@inheritDoc} */
    @Override
    public Flow.Publisher<BinaryRow> scan(int p, InternalTransaction tx) {
        throw new IgniteInternalException(new OperationNotSupportedException());
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull List<String> assignments() {
        throw new IgniteInternalException(new OperationNotSupportedException());
    }

    /** {@inheritDoc} */
    @Override
    public int partition(BinaryRowEx keyRow) {
        return 0;
    }
}
