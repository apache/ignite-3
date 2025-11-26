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

package org.apache.ignite.internal.lowwatermark;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.lowwatermark.LowWatermarkImpl.LOW_WATERMARK_VAULT_KEY;
import static org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent.LOW_WATERMARK_CHANGED;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.trueCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
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
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.lowwatermark.event.ChangeLowWatermarkEventParameters;
import org.apache.ignite.internal.lowwatermark.message.GetLowWatermarkRequest;
import org.apache.ignite.internal.lowwatermark.message.GetLowWatermarkResponse;
import org.apache.ignite.internal.lowwatermark.message.LowWatermarkMessageGroup;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.schema.configuration.LowWatermarkConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
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

    private final VaultManager vaultManager = mock(VaultManager.class);

    private final FailureManager failureManager = mock(FailureManager.class);

    private EventListener<ChangeLowWatermarkEventParameters> lwmChangedListener;

    private LowWatermarkImpl lowWatermark;

    private final MessagingService messagingService = mock(MessagingService.class);

    @BeforeEach
    void setUp() {
        lwmChangedListener = mock(EventListener.class);
        when(lwmChangedListener.notify(any())).thenReturn(falseCompletedFuture());

        lowWatermark = new LowWatermarkImpl(
                "test",
                lowWatermarkConfig,
                clockService,
                vaultManager,
                failureManager,
                messagingService
        );

        lowWatermark.listen(LOW_WATERMARK_CHANGED, lwmChangedListener);
    }

    @AfterEach
    void tearDown() {
        assertThat(lowWatermark.stopAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @Test
    void testStartWithEmptyVault() {
        // Let's check the start with no low watermark in vault.
        assertThat(lowWatermark.startAsync(new ComponentContext()), willCompleteSuccessfully());
        lowWatermark.scheduleUpdates();

        verify(lwmChangedListener, never()).notify(any());

        assertNull(lowWatermark.getLowWatermark());
    }

    @Test
    void testStartWithNotEmptyVault() {
        var lowWatermark = new HybridTimestamp(10, 10);

        when(vaultManager.get(LOW_WATERMARK_VAULT_KEY))
                .thenReturn(new VaultEntry(LOW_WATERMARK_VAULT_KEY, lowWatermark.toBytes()));

        assertThat(this.lowWatermark.startAsync(new ComponentContext()), willCompleteSuccessfully());

        assertEquals(lowWatermark, this.lowWatermark.getLowWatermark());

        var startOnLwnChangedFuture = new CompletableFuture<Void>();

        when(lwmChangedListener.notify(any())).then(invocation -> {
            startOnLwnChangedFuture.complete(null);

            return falseCompletedFuture();
        });

        this.lowWatermark.scheduleUpdates();

        assertThat(startOnLwnChangedFuture, willTimeoutFast());
        assertEquals(lowWatermark, this.lowWatermark.getLowWatermark());
    }

    @Test
    void testCreateNewLowWatermarkCandidate() {
        when(clockService.now()).thenReturn(new HybridTimestamp(1_000, 500));

        assertThat(lowWatermarkConfig.dataAvailabilityTimeMillis().update(100L), willSucceedFast());

        HybridTimestamp newLowWatermarkCandidate = lowWatermark.createNewLowWatermarkCandidate();

        assertThat(newLowWatermarkCandidate.getPhysical(), lessThanOrEqualTo(1_000L - 100));
        assertEquals(500L, newLowWatermarkCandidate.getLogical());
    }

    @Test
    void testUpdateAndNotify() {
        HybridTimestamp now = clockService.now();

        when(clockService.now()).thenReturn(now);

        // Make a predictable candidate to make it easier to test.
        HybridTimestamp newLowWatermarkCandidate = lowWatermark.createNewLowWatermarkCandidate();

        var newLowWatermarkFromChangedListenerFuture = new CompletableFuture<HybridTimestamp>();

        when(lwmChangedListener.notify(any())).then(invocation -> {
            HybridTimestamp newLowWatermark = ((ChangeLowWatermarkEventParameters) invocation.getArgument(0)).newLowWatermark();

            newLowWatermarkFromChangedListenerFuture.complete(newLowWatermark);

            return falseCompletedFuture();
        });

        assertThat(lowWatermark.updateAndNotify(newLowWatermarkCandidate), willCompleteSuccessfully());

        InOrder inOrder = inOrder(vaultManager, lwmChangedListener);

        inOrder.verify(vaultManager, timeout(1_000)).put(LOW_WATERMARK_VAULT_KEY, newLowWatermarkCandidate.toBytes());

        inOrder.verify(lwmChangedListener, timeout(1_000)).notify(any());

        assertEquals(newLowWatermarkCandidate, lowWatermark.getLowWatermark());

        assertThat(newLowWatermarkFromChangedListenerFuture, willBe(newLowWatermarkCandidate));
    }

    /** Let's make sure that the low watermark update happens one by one and not in parallel. */
    @Test
    void testUpdateWatermarkSequentially() throws Exception {
        assertThat(lowWatermarkConfig.updateIntervalMillis().update(10L), willSucceedFast());

        var onLwmChangedLatch = new CountDownLatch(3);

        var onLwmChangedFinishFuture = new CompletableFuture<>();

        var firstOnLwmChangedFuture = new CompletableFuture<>();

        try {
            assertThat(lowWatermarkConfig.updateIntervalMillis().update(100L), willSucceedFast());

            when(lwmChangedListener.notify(any())).then(invocation -> {
                onLwmChangedLatch.countDown();

                firstOnLwmChangedFuture.complete(null);

                return onLwmChangedFinishFuture;
            });

            assertThat(lowWatermark.startAsync(new ComponentContext()), willCompleteSuccessfully());
            lowWatermark.scheduleUpdates();

            // Let's check that it hasn't been called more than once.
            assertThat(firstOnLwmChangedFuture, willCompleteSuccessfully());

            // Let's check that it was called only once.
            assertEquals(2, onLwmChangedLatch.getCount());
            verify(lwmChangedListener).notify(any());

            onLwmChangedFinishFuture.complete(false);

            // Let's make sure that the second time we also went to update the low watermark.
            assertTrue(onLwmChangedLatch.await(1, TimeUnit.SECONDS));

            assertNotNull(lowWatermark.getLowWatermark());
        } finally {
            onLwmChangedFinishFuture.complete(null);
        }
    }

    @Test
    void testHandleGetLowWatermarkMessage() {
        verify(messagingService, never()).addMessageHandler(eq(LowWatermarkMessageGroup.class), any());

        assertThat(lowWatermark.startAsync(new ComponentContext()), willCompleteSuccessfully());

        verify(messagingService).addMessageHandler(eq(LowWatermarkMessageGroup.class), any());

        InternalClusterNode sender = mock(InternalClusterNode.class);
        long correlationId = 0;

        ArgumentCaptor<NetworkMessage> networkMessageArgumentCaptor = ArgumentCaptor.forClass(NetworkMessage.class);

        when(messagingService.respond(any(InternalClusterNode.class), networkMessageArgumentCaptor.capture(), anyLong()))
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
        assertThat(lowWatermark.startAsync(new ComponentContext()), willCompleteSuccessfully());

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

    @Test
    void testGetLowWatermarkFromListener() {
        assertThat(lowWatermark.startAsync(new ComponentContext()), willCompleteSuccessfully());

        HybridTimestamp newLwm = clockService.now();

        lowWatermark.listen(LOW_WATERMARK_CHANGED, (ChangeLowWatermarkEventParameters parameters) -> {
            try {
                assertEquals(newLwm, parameters.newLowWatermark());

                assertEquals(newLwm, lowWatermark.getLowWatermark());

                lowWatermark.getLowWatermarkSafe(lwm -> assertEquals(newLwm, lwm));

                return trueCompletedFuture();
            } catch (Throwable t) {
                return failedFuture(t);
            }
        });

        assertThat(lowWatermark.updateAndNotify(newLwm), willCompleteSuccessfully());
    }

    @Test
    void testUpdateAndNotifyNotInvokeFailureManagerWhenGetNodeStoppingException() {
        lowWatermark.listen(LOW_WATERMARK_CHANGED, parameters -> failedFuture(new NodeStoppingException()));

        assertThat(lowWatermark.updateAndNotify(clockService.now()), willThrowWithCauseOrSuppressed(NodeStoppingException.class));

        verify(failureManager, never()).process(any());
    }

    @Test
    void testParallelScheduleUpdates() throws Exception {
        assertThat(lowWatermarkConfig.updateIntervalMillis().update(300L), willCompleteSuccessfully());

        assertThat(lowWatermark.startAsync(new ComponentContext()), willCompleteSuccessfully());

        runRace(
                () -> lowWatermark.scheduleUpdates(),
                () -> lowWatermark.scheduleUpdates(),
                () -> lowWatermark.scheduleUpdates(),
                () -> lowWatermark.scheduleUpdates()
        );

        Thread.sleep(1_000);

        verify(lwmChangedListener, atLeast(2)).notify(any());
        verify(lwmChangedListener, atMost(4)).notify(any());
    }

    @Test
    void testScheduleUpdatesAfterUpdateIntervalInConfig() {
        assertThat(lowWatermarkConfig.updateIntervalMillis().update(50_000L), willCompleteSuccessfully());

        assertThat(lowWatermark.startAsync(new ComponentContext()), willCompleteSuccessfully());

        assertThat(lowWatermarkConfig.updateIntervalMillis().update(100L), willCompleteSuccessfully());

        verify(lwmChangedListener, timeout(1_000).atLeast(1)).notify(any());
    }

    private CompletableFuture<HybridTimestamp> listenUpdateLowWatermark() {
        var future = new CompletableFuture<HybridTimestamp>();

        lowWatermark.listen(LOW_WATERMARK_CHANGED, (ChangeLowWatermarkEventParameters parameters) -> {
            future.complete(parameters.newLowWatermark());

            return trueCompletedFuture();
        });

        return future;
    }
}
