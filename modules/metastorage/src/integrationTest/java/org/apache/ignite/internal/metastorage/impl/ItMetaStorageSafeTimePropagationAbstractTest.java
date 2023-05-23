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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Safe time propagation tests. */
public abstract class ItMetaStorageSafeTimePropagationAbstractTest {

    private KeyValueStorage storage;

    private final HybridClock clock = new HybridClockImpl();

    private final ClusterTimeImpl time = new ClusterTimeImpl(new IgniteSpinBusyLock(), clock);

    @BeforeEach
    public void setUp() {
        storage = createStorage();

        storage.start();

        storage.startWatches((e, t) -> {
            time.updateSafeTime(t);

            return CompletableFuture.completedFuture(null);
        });
    }

    @AfterEach
    void tearDown() throws Exception {
        storage.close();
    }

    @Test
    public void testTimePropagated() throws Exception {
        CompletableFuture<Void> f = new CompletableFuture<>();
        CountDownLatch latch = new CountDownLatch(1);

        storage.watchExact(key(0), 1, new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent event) {
                latch.countDown();
                return f;
            }

            @Override
            public void onError(Throwable e) {
                // No-op.
            }
        });

        HybridTimestamp opTs = clock.now();

        storage.put(key(0), keyValue(0, 1), opTs);

        assertTrue(latch.await(1, TimeUnit.SECONDS));

        f.complete(null);

        assertThat(time.waitFor(opTs), willCompleteSuccessfully());
    }

    /**
     * Returns key value storage for this test.
     */
    abstract KeyValueStorage createStorage();

    protected static byte[] key(int k) {
        return ("key" + k).getBytes(UTF_8);
    }

    protected static byte[] keyValue(int k, int v) {
        return ("key" + k + '_' + "val" + v).getBytes(UTF_8);
    }

}
