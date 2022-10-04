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

package org.apache.ignite.internal.table.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.naming.OperationNotSupportedException;
import org.apache.ignite.hlc.HybridClock;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.impl.TestMvPartitionStorage;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.replicator.PartitionReplicaListener;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestConcurrentHashMapTxStateStorage;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.Peer;
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

    private ReplicaListener replicaListener;

    private String groupId;

    /**
     * Creates a new local table.
     *
     * @param replicaSvc Replica service.
     */
    public DummyInternalTableImpl(ReplicaService replicaSvc) {
        this(replicaSvc, new TestMvPartitionStorage(0));
    }

    /**
     * Creates a new local table.
     *
     * @param replicaSvc Replica service.
     * @param txManager Transaction manager.
     * @param crossTableUsage If this dummy table is going to be used in cross-table tests, it won't mock the calls of ReplicaService
     *                        by itself.
     */
    public DummyInternalTableImpl(ReplicaService replicaSvc, TxManager txManager, boolean crossTableUsage) {
        this(replicaSvc, new TestMvPartitionStorage(0), txManager, crossTableUsage);
    }

    /**
     * Creates a new local table.
     *
     * @param replicaSvc Replica service.
     * @param mvPartStorage Multi version partition storage.
     */
    public DummyInternalTableImpl(ReplicaService replicaSvc, MvPartitionStorage mvPartStorage) {
        this(replicaSvc, mvPartStorage, null, false);
    }

    /**
     * Creates a new local table.
     *
     * @param replicaSvc Replica service.
     * @param mvPartStorage Multi version partition storage.
     * @param txManager Transaction manager, if {@code null}, then default one will be created.
     * @param crossTableUsage If this dummy table is going to be used in cross-table tests, it won't mock the calls of ReplicaService
     *                        by itself.
     */
    public DummyInternalTableImpl(
            ReplicaService replicaSvc,
            MvPartitionStorage mvPartStorage,
            @Nullable TxManager txManager,
            boolean crossTableUsage
    ) {
        super(
                "test",
                UUID.randomUUID(),
                Int2ObjectMaps.singleton(0, mock(RaftGroupService.class)),
                1,
                NetworkAddress::toString,
                addr -> Mockito.mock(ClusterNode.class),
                txManager == null ? new TxManagerImpl(replicaSvc, new HeapLockManager(), mock(HybridClock.class)) : txManager,
                mock(MvTableStorage.class),
                mock(TxStateTableStorage.class),
                replicaSvc,
                mock(HybridClock.class)
        );
        RaftGroupService svc = partitionMap.get(0);

        groupId = crossTableUsage ? "testGrp-" + UUID.randomUUID() : "testGrp";

        lenient().doReturn(groupId).when(svc).groupId();
        Peer leaderPeer = new Peer(ADDR);
        lenient().doReturn(leaderPeer).when(svc).leader();
        lenient().doReturn(CompletableFuture.completedFuture(new IgniteBiTuple<>(leaderPeer, 1L))).when(svc).refreshAndGetLeaderWithTerm();

        if (!crossTableUsage) {
            // Delegate replica requests directly to replica listener.
            lenient().doAnswer(
                    invocationOnMock -> {
                        CompletableFuture<Object> invoke = replicaListener.invoke(invocationOnMock.getArgument(1));
                        return invoke;
                    }
            ).when(replicaSvc).invoke(any(), any());
        }

        AtomicLong raftIndex = new AtomicLong();

        // Delegate directly to listener.
        lenient().doAnswer(
                invocationClose -> {
                    Command cmd = invocationClose.getArgument(0);

                    long commandIndex = raftIndex.incrementAndGet();

                    CompletableFuture<Serializable> res = new CompletableFuture<>();

                    // All read commands are handled directly throw partition replica listener.
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
                            if (r instanceof Throwable) {
                                res.completeExceptionally((Throwable) r);
                            } else {
                                res.complete(r);
                            }
                        }
                    };

                    try {
                        partitionListener.onWrite(List.of(clo).iterator());
                    } catch (Throwable e) {
                        res.completeExceptionally(new TransactionException(e));
                    }

                    return res;
                }
        ).when(svc).run(any());

        var primaryIndex = new ConcurrentHashMap<ByteBuffer, RowId>();

        replicaListener = new PartitionReplicaListener(
                mvPartStorage,
                partitionMap.get(0),
                this.txManager,
                this.txManager.lockManager(),
                0,
                groupId,
                tableId(),
                primaryIndex,
                new HybridClock()
        );

        partitionListener = new PartitionListener(
                mvPartStorage,
                new TestConcurrentHashMapTxStateStorage(),
                this.txManager,
                primaryIndex
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

    /**
     * Replica listener.
     *
     * @return Replica listener.
     */
    public ReplicaListener getReplicaListener() {
        return replicaListener;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull UUID tableId() {
        return UUID.randomUUID();
    }

    /**
     * Group id of single partition of this table.
     *
     * @return Group id.
     */
    public String groupId() {
        return groupId;
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
    public @NotNull List<String> assignments() {
        throw new IgniteInternalException(new OperationNotSupportedException());
    }

    /** {@inheritDoc} */
    @Override
    public int partition(BinaryRowEx keyRow) {
        return 0;
    }
}
