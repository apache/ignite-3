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

package org.apache.ignite.internal.tx.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessageHandler;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.message.FinishedTransactionsBatchMessage;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FinishedTransactionBatchRequestHandlerTest extends BaseIgniteAbstractTest {
    @Mock
    private MessagingService messagingService;

    @Mock
    private RemotelyTriggeredResourceRegistry resourceRegistry;

    @Mock
    private LowWatermark lowWatermark;

    private FinishedTransactionBatchRequestHandler requestHandler;

    private NetworkMessageHandler networkHandler;

    @BeforeEach
    void createAndStartHandler() {
        requestHandler = new FinishedTransactionBatchRequestHandler(
                messagingService,
                resourceRegistry,
                lowWatermark,
                ForkJoinPool.commonPool()
        );
        requestHandler.start();

        ArgumentCaptor<NetworkMessageHandler> handlerCaptor = ArgumentCaptor.forClass(NetworkMessageHandler.class);
        verify(messagingService).addMessageHandler(eq(TxMessageGroup.class), handlerCaptor.capture());

        networkHandler = handlerCaptor.getValue();
        assertThat(networkHandler, is(notNullValue()));
    }

    @Test
    void unlocksLwm() {
        UUID txId1 = new UUID(1, 1);
        UUID txId2 = new UUID(2, 2);

        FinishedTransactionsBatchMessage message = new TxMessagesFactory().finishedTransactionsBatchMessage()
                .transactions(List.of(txId1, txId2))
                .build();

        networkHandler.onReceived(message, mock(InternalClusterNode.class), null);

        verify(lowWatermark, timeout(10_000)).unlock(txId1);
        verify(lowWatermark, timeout(10_000)).unlock(txId2);
    }
}
