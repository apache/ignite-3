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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.table.distributed.command.FinishTxCommand;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.message.TxFinishRequest;
import org.apache.ignite.internal.tx.message.TxFinishResponse;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.jetbrains.annotations.Nullable;

/**
 * Test utils for adding mock logic to messaging service.
 */
public class MessagingServiceTestUtils {
    /**
     * Mocking of {@link MessagingService#invoke(NetworkAddress, NetworkMessage, long)}.
     *
     * @param messagingService Messaging service.
     * @param txManager Transaction manager.
     * @param partitionListeners Partition listeners.
     */
    public static void mockMessagingServiceInvoke(
            MessagingService messagingService,
            TxManager txManager,
            List<PartitionListener> partitionListeners
    ) {
        doAnswer(
                invocationClose -> {
                    assert invocationClose.getArgument(1) instanceof TxFinishRequest;

                    TxFinishRequest txFinishRequest = invocationClose.getArgument(1);

                    UUID txId = txFinishRequest.txId();

                    txManager.changeState(txId, TxState.PENDING, txFinishRequest.commit() ? TxState.COMMITED : TxState.ABORTED);

                    FinishTxCommand finishTxCommand = new FinishTxCommand(
                            txId, txFinishRequest.commit(), txManager.lockedKeys(txId)
                    );

                    partitionListeners.forEach(partitionListener -> partitionListener.onWrite(iterator(finishTxCommand)));

                    if (txFinishRequest.commit()) {
                        txManager.commitAsync(txId).get();
                    } else {
                        txManager.rollbackAsync(txId).get();
                    }

                    return CompletableFuture.completedFuture(mock(TxFinishResponse.class));
                }
        ).when(messagingService)
                .invoke(any(NetworkAddress.class), any(TxFinishRequest.class), anyLong());
    }

    private static <T extends Command> Iterator<CommandClosure<T>> iterator(T obj) {
        CommandClosure<T> closure = new CommandClosure<>() {
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
