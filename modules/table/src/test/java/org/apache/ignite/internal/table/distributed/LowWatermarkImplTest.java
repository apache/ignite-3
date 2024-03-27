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

package org.apache.ignite.internal.table.distributed;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.table.distributed.LowWatermarkImpl.LOW_WATERMARK_VAULT_KEY;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.schema.configuration.LowWatermarkConfiguration;
import org.apache.ignite.internal.table.distributed.message.GetLowWatermarkRequest;
import org.apache.ignite.internal.table.distributed.message.GetLowWatermarkResponse;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.network.ClusterNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

/** For {@link LowWatermarkImpl} testing. */
@ExtendWith(ConfigurationExtension.class)
public class LowWatermarkImplTest extends BaseIgniteAbstractTest {
    @InjectConfiguration
    private LowWatermarkConfiguration lowWatermarkConfig;

    private final ClockService clockService = spy(new TestClockService(new HybridClockImpl()));

    private final TxManager txManager = mock(TxManager.class);

    private final VaultManager vaultManager = mock(VaultManager.class);

    private LowWatermarkChangedListener listener;

    private LowWatermarkImpl lowWatermark;

    private final MessagingService messagingService = mock(MessagingService.class);

    @BeforeEach
    void setUp() {
        listener = mock(LowWatermarkChangedListener.class);
        when(listener.onLwmChanged(any(HybridTimestamp.class))).thenReturn(nullCompletedFuture());

        lowWatermark = new LowWatermarkImpl(
                "test",
                lowWatermarkConfig,
                clockService,
                txManager,
                vaultManager,
                mock(FailureProcessor.class),
                messagingService
        );

        lowWatermark.addUpdateListener(listener);
    }

    @AfterEach
    void tearDown() {
        lowWatermark.stop();
    }

    @Test
    void testStartWithEmptyVault() {
        // Let's check the start with no low watermark in vault.
        assertThat(lowWatermark.start(), willCompleteSuccessfully());
        lowWatermark.scheduleUpdates();

        verify(listener, never()).onLwmChanged(any(HybridTimestamp.class));
        assertNull(lowWatermark.getLowWatermark());
    }

    @Test
    void testStartWithNotEmptyVault() {
        HybridTimestamp lowWatermark = new HybridTimestamp(10, 10);

        when(vaultManager.get(LOW_WATERMARK_VAULT_KEY))
                .thenReturn(new VaultEntry(LOW_WATERMARK_VAULT_KEY, ByteUtils.toBytes(lowWatermark)));

        when(txManager.updateLowWatermark(any(HybridTimestamp.class))).thenReturn(nullCompletedFuture());

        assertThat(this.lowWatermark.start(), willCompleteSuccessfully());

        assertEquals(lowWatermark, this.lowWatermark.getLowWatermark());

        this.lowWatermark.scheduleUpdates();

        verify(listener, timeout(1_000)).onLwmChanged(lowWatermark);
        assertEquals(lowWatermark, this.lowWatermark.getLowWatermark());
    }

    @Test
    void testCreateNewLowWatermarkCandidate() {
        when(clockService.now()).thenReturn(new HybridTimestamp(1_000, 500));

        assertThat(lowWatermarkConfig.dataAvailabilityTime().update(100L), willSucceedFast());

        HybridTimestamp newLowWatermarkCandidate = lowWatermark.createNewLowWatermarkCandidate();

        assertThat(newLowWatermarkCandidate.getPhysical(), lessThanOrEqualTo(1_000L - 100));
        assertEquals(500L, newLowWatermarkCandidate.getLogical());
    }

    @Test
    void testUpdateAndNotify() {
        HybridTimestamp now = clockService.now();

        when(clockService.now()).thenReturn(now);

        when(txManager.updateLowWatermark(any(HybridTimestamp.class))).thenReturn(nullCompletedFuture());

        // Make a predictable candidate to make it easier to test.
        HybridTimestamp newLowWatermarkCandidate = lowWatermark.createNewLowWatermarkCandidate();

        assertThat(lowWatermark.updateAndNotify(newLowWatermarkCandidate), willCompleteSuccessfully());

        InOrder inOrder = inOrder(txManager, vaultManager, listener);

        inOrder.verify(txManager).updateLowWatermark(newLowWatermarkCandidate);

        inOrder.verify(vaultManager, timeout(1000)).put(LOW_WATERMARK_VAULT_KEY, ByteUtils.toBytes(newLowWatermarkCandidate));

        inOrder.verify(listener, timeout(1_000)).onLwmChanged(newLowWatermarkCandidate);

        assertEquals(newLowWatermarkCandidate, lowWatermark.getLowWatermark());
    }

    /**
     * Let's make sure that the low watermark update happens one by one and not in parallel.
     */
    @Test
    void testUpdateWatermarkSequentially() throws Exception {
        assertThat(lowWatermarkConfig.updateFrequency().update(10L), willSucceedFast());

        CountDownLatch startGetAllReadOnlyTransactions = new CountDownLatch(3);

        CompletableFuture<Void> finishGetAllReadOnlyTransactions = new CompletableFuture<>();

        assertThat(lowWatermarkConfig.updateFrequency().update(100L), willSucceedFast());

        try {
            when(txManager.updateLowWatermark(any(HybridTimestamp.class))).then(invocation -> {
                startGetAllReadOnlyTransactions.countDown();

                return finishGetAllReadOnlyTransactions;
            });

            assertThat(lowWatermark.start(), willCompleteSuccessfully());
            lowWatermark.scheduleUpdates();

            // Let's check that it hasn't been called more than once.
            assertFalse(startGetAllReadOnlyTransactions.await(1, TimeUnit.SECONDS));

            // Let's check that it was called only once.
            assertEquals(2, startGetAllReadOnlyTransactions.getCount());
            verify(txManager).updateLowWatermark(any(HybridTimestamp.class));

            finishGetAllReadOnlyTransactions.complete(null);

            // Let's make sure that the second time we also went to update the low watermark.
            assertTrue(startGetAllReadOnlyTransactions.await(1, TimeUnit.SECONDS));

            assertNotNull(lowWatermark.getLowWatermark());
        } finally {
            finishGetAllReadOnlyTransactions.complete(null);
        }
    }

    @Test
    void testHandleGetLowWatermarkMessage() {
        when(txManager.updateLowWatermark(any())).thenReturn(nullCompletedFuture());

        verify(messagingService, never()).addMessageHandler(eq(TableMessageGroup.class), any());

        assertThat(lowWatermark.start(), willCompleteSuccessfully());

        verify(messagingService).addMessageHandler(eq(TableMessageGroup.class), any());

        ClusterNode sender = mock(ClusterNode.class);
        long correlationId = 0;

        ArgumentCaptor<NetworkMessage> networkMessageArgumentCaptor = ArgumentCaptor.forClass(NetworkMessage.class);

        when(messagingService.respond(any(ClusterNode.class), networkMessageArgumentCaptor.capture(), anyLong()))
                .thenReturn(nullCompletedFuture());

        // Let's check any message except GetLowWatermarkRequest.
        lowWatermark.onReceiveNetworkMessage(mock(NetworkMessage.class), sender, correlationId);
        assertThat(networkMessageArgumentCaptor.getAllValues(), empty());

        lowWatermark.onReceiveNetworkMessage(mock(GetLowWatermarkRequest.class), sender, correlationId);

        assertThat(lowWatermark.updateAndNotify(lowWatermark.createNewLowWatermarkCandidate()), willCompleteSuccessfully());

        lowWatermark.onReceiveNetworkMessage(mock(GetLowWatermarkRequest.class), sender, correlationId);

        List<HybridTimestamp> respondedLowWatermarks = networkMessageArgumentCaptor.getAllValues().stream()
                .map(GetLowWatermarkResponse.class::cast)
                .map(GetLowWatermarkResponse::lowWatermark)
                .map(HybridTimestamp::nullableHybridTimestamp)
                .collect(toList());

        assertThat(respondedLowWatermarks, contains(null, lowWatermark.getLowWatermark()));
    }

    @Test
    void testUpdateLowWatermark() {
        when(txManager.updateLowWatermark(any())).thenReturn(nullCompletedFuture());

        assertThat(lowWatermark.start(), willCompleteSuccessfully());

        CompletableFuture<HybridTimestamp> updateLowWatermarkFuture0 = listenUpdateLowWatermark();

        HybridTimestamp newLwm0 = clockService.now();

        lowWatermark.updateLowWatermark(newLwm0);

        assertThat(updateLowWatermarkFuture0, willBe(newLwm0));

        // Let's check it again.
        CompletableFuture<HybridTimestamp> updateLowWatermarkFuture1 = listenUpdateLowWatermark();

        HybridTimestamp newLwm1 = clockService.now();

        lowWatermark.updateLowWatermark(newLwm1);

        assertThat(updateLowWatermarkFuture1, willBe(newLwm1));

        // Let's check the unchanged value.
        CompletableFuture<HybridTimestamp> updateLowWatermarkFuture2 = listenUpdateLowWatermark();

        lowWatermark.updateLowWatermark(newLwm1);

        assertThat(updateLowWatermarkFuture2, willTimeoutFast());
    }

    private CompletableFuture<HybridTimestamp> listenUpdateLowWatermark() {
        var future = new CompletableFuture<HybridTimestamp>();

        lowWatermark.addUpdateListener(ts -> {
            future.complete(ts);

            return nullCompletedFuture();
        });

        return future;
    }
}
