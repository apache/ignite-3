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

package org.apache.ignite.internal.partition.replicator.raft;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.FailureType;
import org.apache.ignite.internal.failure.handlers.FailureHandler;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl.DelegatingStateMachine;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.tx.TransactionResult;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.junit.jupiter.api.Test;

class ExpectedTxStateRaceFailureHandlerTest {
    @Test
    void unexpectedTxStateDoesNotTriggerFailureHandler() {
        AtomicBoolean reached = new AtomicBoolean();

        RaftGroupListener listener = new RaftGroupListener() {
            @Override
            public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
                // No-op.
            }

            @Override
            public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
                throw new UnexpectedTransactionStateException(
                        "Expected tx state race",
                        new TransactionResult(TxState.COMMITTED, new HybridTimestamp(1, 1))
                );
            }

            @Override
            public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
                // No-op.
            }

            @Override
            public boolean onSnapshotLoad(Path path) {
                return true;
            }

            @Override
            public void onShutdown() {
                // No-op.
            }
        };

        DelegatingStateMachine sm = new JraftServerImpl.DelegatingStateMachine(
                new RaftNodeId(new ZonePartitionId(0, 0), new Peer("test")),
                listener,
                mock(NodeOptions.class),
                new FailureManager(new FailureHandler() {
                    @Override
                    public boolean onFailure(FailureContext failureCtx) {
                        reached.set(true);
                        return false;
                    }

                    @Override
                    public Set<FailureType> ignoredFailureTypes() {
                        return Set.of();
                    }
                })
        );

        sm.onApply(mock(org.apache.ignite.raft.jraft.Iterator.class));

        assertFalse(reached.get());
    }

    @Test
    void otherExceptionsTriggerFailureHandler() {
        AtomicBoolean reached = new AtomicBoolean();

        RaftGroupListener listener = new RaftGroupListener() {
            @Override
            public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
                // No-op.
            }

            @Override
            public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
                throw new RuntimeException("Boom");
            }

            @Override
            public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
                // No-op.
            }

            @Override
            public boolean onSnapshotLoad(Path path) {
                return true;
            }

            @Override
            public void onShutdown() {
                // No-op.
            }
        };

        DelegatingStateMachine sm = new JraftServerImpl.DelegatingStateMachine(
                new RaftNodeId(new ZonePartitionId(0, 0), new Peer("test")),
                listener,
                mock(NodeOptions.class),
                new FailureManager(new FailureHandler() {
                    @Override
                    public boolean onFailure(FailureContext failureCtx) {
                        reached.set(true);
                        return false;
                    }

                    @Override
                    public Set<FailureType> ignoredFailureTypes() {
                        return Set.of();
                    }
                })
        );

        sm.onApply(mock(org.apache.ignite.raft.jraft.Iterator.class));

        assertTrue(reached.get());
    }
}

