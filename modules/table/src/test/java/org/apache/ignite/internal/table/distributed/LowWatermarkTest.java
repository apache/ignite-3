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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.table.distributed.LowWatermark.LOW_WATERMARK_VAULT_KEY;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.configuration.LowWatermarkConfiguration;
import org.apache.ignite.internal.table.distributed.gc.MvGc;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;

/**
 * For {@link LowWatermark} testing.
 */
@ExtendWith(ConfigurationExtension.class)
public class LowWatermarkTest extends BaseIgniteAbstractTest {
    @InjectConfiguration
    private LowWatermarkConfiguration lowWatermarkConfig;

    private final HybridClock clock = spy(new HybridClockImpl());

    private final TxManager txManager = mock(TxManager.class);

    private final VaultManager vaultManager = mock(VaultManager.class);

    private final MvGc mvGc = mock(MvGc.class);

    private LowWatermark lowWatermark;

    @BeforeEach
    void setUp() {
        lowWatermark = new LowWatermark("test", lowWatermarkConfig, clock, txManager, vaultManager, mvGc);
    }

    @AfterEach
    void tearDown() throws Exception {
        lowWatermark.close();
    }

    @Test
    void testStartWithEmptyVault() {
        // Let's check the start with no low watermark in vault.
        when(vaultManager.get(LOW_WATERMARK_VAULT_KEY)).thenReturn(completedFuture(null));

        lowWatermark.start();

        verify(mvGc, never()).updateLowWatermark(any(HybridTimestamp.class));
        assertNull(lowWatermark.getLowWatermark());
    }

    @Test
    void testStartWithNotEmptyVault() {
        HybridTimestamp lowWatermark = new HybridTimestamp(10, 10);

        when(vaultManager.get(LOW_WATERMARK_VAULT_KEY))
                .thenReturn(completedFuture(new VaultEntry(LOW_WATERMARK_VAULT_KEY, ByteUtils.toBytes(lowWatermark))));

        when(txManager.updateLowWatermark(any(HybridTimestamp.class))).thenReturn(completedFuture(null));

        this.lowWatermark.start();

        verify(mvGc).updateLowWatermark(lowWatermark);
        assertEquals(lowWatermark, this.lowWatermark.getLowWatermark());
    }

    @Test
    void testCreateNewLowWatermarkCandidate() {
        when(clock.now()).thenReturn(new HybridTimestamp(1_000, 500));

        assertThat(lowWatermarkConfig.dataAvailabilityTime().update(100L), willSucceedFast());

        HybridTimestamp newLowWatermarkCandidate = lowWatermark.createNewLowWatermarkCandidate();

        assertThat(newLowWatermarkCandidate.getPhysical(), lessThanOrEqualTo(1_000L - 100));
        assertEquals(500L, newLowWatermarkCandidate.getLogical());
    }

    @Test
    void testUpdateLowWatermark() {
        HybridTimestamp now = clock.now();

        when(clock.now()).thenReturn(now);

        when(txManager.updateLowWatermark(any(HybridTimestamp.class))).thenReturn(completedFuture(null));

        when(vaultManager.put(any(ByteArray.class), any(byte[].class))).thenReturn(completedFuture(null));

        // Make a predictable candidate to make it easier to test.
        HybridTimestamp newLowWatermarkCandidate = lowWatermark.createNewLowWatermarkCandidate();

        lowWatermark.updateLowWatermark();

        InOrder inOrder = inOrder(txManager, vaultManager, mvGc);

        inOrder.verify(txManager).updateLowWatermark(newLowWatermarkCandidate);

        inOrder.verify(vaultManager, timeout(1000)).put(LOW_WATERMARK_VAULT_KEY, ByteUtils.toBytes(newLowWatermarkCandidate));

        inOrder.verify(mvGc).updateLowWatermark(newLowWatermarkCandidate);

        assertEquals(newLowWatermarkCandidate, lowWatermark.getLowWatermark());
    }

    /**
     * Let's make sure that the low watermark update happens one by one and not in parallel.
     */
    @Test
    void testUpdateWatermarkSequentially() throws Exception {
        when(vaultManager.get(LOW_WATERMARK_VAULT_KEY)).thenReturn(completedFuture(null));
        assertThat(lowWatermarkConfig.updateFrequency().update(10L), willSucceedFast());

        when(vaultManager.put(any(ByteArray.class), any(byte[].class))).thenReturn(completedFuture(null));

        CountDownLatch startGetAllReadOnlyTransactions = new CountDownLatch(3);

        CompletableFuture<Void> finishGetAllReadOnlyTransactions = new CompletableFuture<>();

        assertThat(lowWatermarkConfig.updateFrequency().update(100L), willSucceedFast());

        try {
            when(txManager.updateLowWatermark(any(HybridTimestamp.class))).then(invocation -> {
                startGetAllReadOnlyTransactions.countDown();

                return finishGetAllReadOnlyTransactions;
            });

            lowWatermark.start();

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
}
