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

package org.apache.ignite.internal.network.recovery;

import static org.apache.ignite.internal.testframework.asserts.CompletableFutureAssert.assertWillThrowFast;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.RecipientLeftException;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.internal.network.netty.NettySender;
import org.apache.ignite.internal.network.recovery.message.HandshakeRejectedMessage;
import org.apache.ignite.internal.network.recovery.message.HandshakeRejectionReason;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({MockitoExtension.class, ConfigurationExtension.class})
abstract class HandshakeManagerTest extends BaseIgniteAbstractTest {
    protected static final NetworkMessagesFactory MESSAGE_FACTORY = new NetworkMessagesFactory();

    protected static void assertThatRejectionWithStoppingCausesRecipientLeftException(HandshakeManager manager) {
        CompletableFuture<NettySender> localHandshakeFuture = manager.localHandshakeFuture();
        CompletionStage<NettySender> finalHandshakeFuture = manager.finalHandshakeFuture();

        manager.onMessage(handshakeRejectedMessageWithReason(HandshakeRejectionReason.STOPPING));

        assertWillThrowFast(localHandshakeFuture, RecipientLeftException.class);
        assertWillThrowFast(finalHandshakeFuture.toCompletableFuture(), RecipientLeftException.class);
    }

    private static HandshakeRejectedMessage handshakeRejectedMessageWithReason(HandshakeRejectionReason reason) {
        return MESSAGE_FACTORY.handshakeRejectedMessage()
                .message("Rejected")
                .reasonString(reason.toString())
                .build();
    }
}
