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
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import org.apache.ignite.internal.table.distributed.command.FinishTxCommand;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.message.TxFinishRequest;
import org.apache.ignite.internal.tx.message.TxFinishResponse;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.mockito.Mockito;

/**
 * Test utils for mocking messaging service.
 */
public class MessagingServiceTestUtils {
    /**
     * Prepares messaging service mock.
     *
     * @param txManager Transaction manager.
     * @return Messaging service mock.
     */
    public static MessagingService mockMessagingService(
            ClusterService clusterService,
            TxManager txManager,
            List<PartitionListener> partitionListeners
    ) {
        MessagingService messagingService = Mockito.mock(MessagingService.class, RETURNS_DEEP_STUBS);

        Mockito.when(clusterService.messagingService()).thenReturn(messagingService);

        doAnswer(
                invocationClose -> {
                    assert invocationClose.getArgument(1) instanceof TxFinishRequest;

                    TxFinishRequest txFinishRequest = invocationClose.getArgument(1);

                    FinishTxCommand finishTxCommand = new FinishTxCommand(
                            txFinishRequest.txId(), txFinishRequest.commit(), txManager.lockedKeys(txFinishRequest.txId())
                    );

                    partitionListeners.forEach(partitionListener -> partitionListener.onWrite(iterator((i, clo) -> {
                        when(clo.command()).thenReturn(finishTxCommand);

                        doAnswer(invocation -> null).when(clo).result(any());
                    }))
                    );

                    if (txFinishRequest.commit()) {
                        txManager.commitAsync(txFinishRequest.txId()).get();
                    } else {
                        txManager.rollbackAsync(txFinishRequest.txId()).get();
                    }

                    return CompletableFuture.completedFuture(mock(TxFinishResponse.class));
                }
        ).when(messagingService)
                .invoke(any(NetworkAddress.class), any(TxFinishRequest.class), anyLong());

        return messagingService;
    }

    private static <T extends Command> Iterator<CommandClosure<T>> iterator(BiConsumer<Integer, CommandClosure<T>> func) {
        return new Iterator<>() {
            /** Iteration. */
            private int it = 0;

            /** {@inheritDoc} */
            @Override
            public boolean hasNext() {
                return it++ < 1;
            }

            /** {@inheritDoc} */
            @Override
            public CommandClosure<T> next() {
                CommandClosure<T> clo = mock(CommandClosure.class);

                func.accept(it, clo);

                it++;

                return clo;
            }
        };
    }
}
