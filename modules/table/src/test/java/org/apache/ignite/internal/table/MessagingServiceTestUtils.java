/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.table;

import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.ignite.internal.table.distributed.command.FinishTxCommand;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.message.TxFinishRequest;
import org.apache.ignite.internal.tx.message.TxFinishResponse;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.jetbrains.annotations.Nullable;
import org.mockito.Mockito;

/**
 * Test utils for mocking messaging service.
 */
public class MessagingServiceTestUtils {
    /**
     * Prepares messaging service mock.
     *
     * @param txManager Transaction manager.
     * @param partitionListeners Partition listeners.
     * @return Messaging service mock.
     */
    public static MessagingService mockMessagingService(
            TxManager txManager,
            List<PartitionListener> partitionListeners,
            Function<PartitionListener, AtomicLong> raftIndexFactory
    ) {
        MessagingService messagingService = Mockito.mock(MessagingService.class, RETURNS_DEEP_STUBS);

        doAnswer(
                invocationClose -> {
                    assert invocationClose.getArgument(1) instanceof TxFinishRequest;

                    TxFinishRequest txFinishRequest = invocationClose.getArgument(1);

                    UUID txId = txFinishRequest.txId();

                    txManager.changeState(txId, TxState.PENDING, txFinishRequest.commit() ? TxState.COMMITED : TxState.ABORTED);

                    FinishTxCommand finishTxCommand = new FinishTxCommand(
                            txId, txFinishRequest.commit(), txManager.lockedKeys(txId)
                    );

                    partitionListeners.forEach(partitionListener ->
                            partitionListener.onWrite(iterator(finishTxCommand, raftIndexFactory.apply(partitionListener)))
                    );

                    if (txFinishRequest.commit()) {
                        txManager.commitAsync(txId).get();
                    } else {
                        txManager.rollbackAsync(txId).get();
                    }

                    return CompletableFuture.completedFuture(mock(TxFinishResponse.class));
                }
        ).when(messagingService)
                .invoke(any(NetworkAddress.class), any(TxFinishRequest.class), anyLong());

        return messagingService;
    }

    private static <T extends Command> Iterator<CommandClosure<T>> iterator(T obj, AtomicLong raftIndex) {
        long index = raftIndex.incrementAndGet();

        CommandClosure<T> closure = new CommandClosure<>() {
            /** {@inheritDoc} */
            @Override
            public long index() {
                return index;
            }

            @Override
            public T command() {
                return obj;
            }

            @Override
            public void result(@Nullable Serializable res) {
                // no-op.
            }
        };

        return List.of(closure).iterator();
    }
}
