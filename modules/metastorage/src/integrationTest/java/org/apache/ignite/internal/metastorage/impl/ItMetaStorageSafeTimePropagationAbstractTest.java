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

package org.apache.ignite.internal.metastorage.impl;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.server.AbstractKeyValueStorageTest;
import org.apache.ignite.internal.metastorage.server.OnRevisionAppliedCallback;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Safe time propagation tests. */
public abstract class ItMetaStorageSafeTimePropagationAbstractTest extends AbstractKeyValueStorageTest {
    private final HybridClock clock = new HybridClockImpl();

    private final ClusterTimeImpl time = new ClusterTimeImpl("node", new IgniteSpinBusyLock(), clock);

    @BeforeEach
    public void startWatches() {
        storage.startWatches(1, new OnRevisionAppliedCallback() {
            @Override
            public void onSafeTimeAdvanced(HybridTimestamp newSafeTime) {
                time.updateSafeTime(newSafeTime);
            }

            @Override
            public void onRevisionApplied(long revision) {
                // No-op.
            }
        });
    }

    @AfterEach
    public void stopTime() throws Exception {
        time.close();
    }

    @Test
    public void testTimePropagated() throws Exception {
        CompletableFuture<Void> watchCompletedFuture = new CompletableFuture<>();

        CountDownLatch watchCalledLatch = new CountDownLatch(1);

        // Register watch listener, so that we can control safe time propagation.
        // Safe time can only be propagated when all of the listeners completed their futures successfully.
        storage.watchExact(key(0), 1, new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent event) {
                watchCalledLatch.countDown();
                return watchCompletedFuture;
            }

            @Override
            public void onError(Throwable e) {
                // No-op.
            }
        });

        HybridTimestamp opTs = clock.now();

        storage.put(key(0), keyValue(0, 1), opTs);

        // Ensure watch listener is called.
        assertTrue(watchCalledLatch.await(1, TimeUnit.SECONDS));

        // Safe time must not be propagated before watch notifies all listeners.
        assertThat(time.currentSafeTime(), lessThan(opTs));

        // Finish listener notification.
        watchCompletedFuture.complete(null);

        // Safe time must be propagated.
        assertThat(time.waitFor(opTs), willCompleteSuccessfully());
    }
}
