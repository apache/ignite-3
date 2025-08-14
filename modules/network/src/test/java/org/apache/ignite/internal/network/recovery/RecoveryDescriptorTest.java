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

import static java.util.Collections.emptyList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureCompletedMatcher.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.OutNetworkObject;
import org.apache.ignite.internal.network.netty.NettySender;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RecoveryDescriptorTest extends BaseIgniteAbstractTest {
    private final RecoveryDescriptor descriptor = new RecoveryDescriptor(100);

    @Mock
    private Channel channel1;
    @Mock
    private Channel channel2;

    @Mock
    private ChannelHandlerContext context1;
    @Mock
    private ChannelHandlerContext context2;

    private final CompletableFuture<NettySender> handshakeCompleteFuture1 = new CompletableFuture<>();
    private final CompletableFuture<NettySender> handshakeCompleteFuture2 = new CompletableFuture<>();

    @BeforeEach
    void setupMocks() {
        lenient().when(context1.channel()).thenReturn(channel1);
        lenient().when(context2.channel()).thenReturn(channel2);
    }

    @Test
    void acquiresNonAcquiredDescriptor() {
        assertTrue(descriptor.tryAcquire(context1, handshakeCompleteFuture1));
    }

    @Test
    void acquiryIsAbsentOnNonAcquiredDescriptor() {
        assertThat(descriptor.holder(), is(nullValue()));
    }

    @Test
    void acquiryInformationIsAvailabeAfterAcquiring() {
        descriptor.tryAcquire(context1, handshakeCompleteFuture1);

        DescriptorAcquiry acquiry = descriptor.holder();
        assertThat(acquiry, is(notNullValue()));
        assertThat(acquiry.channel(), is(channel1));
        assertThat(acquiry.clinchResolved().toCompletableFuture().isDone(), is(false));
    }

    @Test
    void cannotAcquireAcquiredDescriptor() {
        descriptor.tryAcquire(context1, handshakeCompleteFuture1);

        assertFalse(descriptor.tryAcquire(context2, handshakeCompleteFuture2));
        assertThat(descriptor.holder().channel(), is(channel1));
    }

    @Test
    void releaseMakesDescriptorAvailable() {
        descriptor.tryAcquire(context1, handshakeCompleteFuture1);

        descriptor.release(context1);

        assertTrue(descriptor.tryAcquire(context1, handshakeCompleteFuture1));
        DescriptorAcquiry acquiry = descriptor.holder();
        assertThat(acquiry, is(notNullValue()));
        assertThat(acquiry.channel(), is(channel1));
    }

    @Test
    void releaseRemovesAcquiryInformation() {
        descriptor.tryAcquire(context1, handshakeCompleteFuture1);

        descriptor.release(context1);

        assertThat(descriptor.holder(), is(nullValue()));
    }

    @Test
    void releaseWithAnotherContextHasNoEffect() {
        descriptor.tryAcquire(context1, handshakeCompleteFuture1);

        descriptor.release(context2);

        DescriptorAcquiry acquiry = descriptor.holder();
        assertThat(acquiry, is(notNullValue()));
        assertThat(acquiry.channel(), is(channel1));
        assertThat(acquiry.clinchResolved().toCompletableFuture().isDone(), is(false));

        assertFalse(descriptor.tryAcquire(context2, handshakeCompleteFuture2));

        acquiry = descriptor.holder();
        assertThat(acquiry, is(notNullValue()));
        assertThat(acquiry.channel(), is(channel1));
        assertThat(acquiry.clinchResolved().toCompletableFuture().isDone(), is(false));
    }

    @Test
    void releaseCompletesClinchReleasedStage() {
        descriptor.tryAcquire(context1, handshakeCompleteFuture1);
        CompletionStage<Void> clinchResolved = descriptor.holder().clinchResolved();

        descriptor.release(context1);

        assertThat(clinchResolved.toCompletableFuture(), is(completedFuture()));
    }

    @Test
    void completesOutObjectFutureOnAcknowledge() {
        OutNetworkObject outObj = new OutNetworkObject(mock(NetworkMessage.class), emptyList());
        descriptor.add(outObj);

        descriptor.acknowledge(1);

        assertThat(outObj.acknowledgedFuture(), is(completedFuture()));
    }

    @Test
    void onlyCompletesFuturesOfAcknowledgedOutObjects() {
        OutNetworkObject outObj1 = new OutNetworkObject(mock(NetworkMessage.class), emptyList());
        OutNetworkObject outObj2 = new OutNetworkObject(mock(NetworkMessage.class), emptyList());
        descriptor.add(outObj1);
        descriptor.add(outObj2);

        descriptor.acknowledge(1);

        assertThat(outObj2.acknowledgedFuture(), is(not(completedFuture())));
    }
}
