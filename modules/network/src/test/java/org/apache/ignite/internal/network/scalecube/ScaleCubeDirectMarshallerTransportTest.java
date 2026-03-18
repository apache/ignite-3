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

package org.apache.ignite.internal.network.scalecube;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import io.scalecube.cluster.transport.api.Message;
import io.scalecube.net.Address;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ScaleCubeDirectMarshallerTransportTest extends BaseIgniteAbstractTest {
    @Mock
    private MessagingService messagingService;

    private ScaleCubeDirectMarshallerTransport transport;

    @BeforeEach
    void createAndStartTransport() {
        transport = new ScaleCubeDirectMarshallerTransport(
                Address.create("localhost", 3000),
                messagingService,
                new NetworkMessagesFactory()
        );

        transport.start().block();
    }

    @AfterEach
    void stopTransport() {
        if (transport != null) {
            transport.stop().block();
        }
    }

    @Test
    void transportSendsByAddress() {
        when(messagingService.send(any(NetworkAddress.class), any(), any())).thenReturn(nullCompletedFuture());

        CompletableFuture<Void> future = transport.send(Address.create("localhost", 3001), Message.withData("test").build()).toFuture();

        assertThat(future, willCompleteSuccessfully());
    }
}
