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
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent.LOW_WATERMARK_CHANGED;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.lowwatermark.event.ChangeLowWatermarkEventParameters;
import org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent;
import org.apache.ignite.internal.lowwatermark.event.LowWatermarkEventParameters;
import org.jetbrains.annotations.Nullable;

/**
 * Low watermark dummy implementation, which requires explicit {@link #updateAndNotify(HybridTimestamp)} method call to notify listeners.
 * This implementation has no persistent state and notifies listeners instantly in same thread.
 */
public class TestLowWatermark extends AbstractEventProducer<LowWatermarkEvent, LowWatermarkEventParameters> implements LowWatermark {
    private static final IgniteLogger LOG = Loggers.forClass(TestLowWatermark.class);

    private volatile @Nullable HybridTimestamp ts;

    private final ReadWriteLock updateLowWatermarkLock = new ReentrantReadWriteLock();

    private final Map<UUID, LowWatermarkLock> locks = new ConcurrentHashMap<>();

    @Override
    public @Nullable HybridTimestamp getLowWatermark() {
        return ts;
    }

    @Override
    public void getLowWatermarkSafe(Consumer<@Nullable HybridTimestamp> consumer) {
        updateLowWatermarkLock.readLock().lock();

        try {
            consumer.accept(ts);
        } finally {
            updateLowWatermarkLock.readLock().unlock();
        }
    }

    @Override
    public void updateLowWatermark(HybridTimestamp newLowWatermark) {
        var currentTs = ts;

        if (currentTs == null || newLowWatermark.compareTo(currentTs) > 0) {
            supplyAsync(() -> updateAndNotifyInternal(newLowWatermark))
                    .thenCompose(Function.identity())
                    .whenComplete((unused, throwable) -> {
                        if (throwable != null) {
                            LOG.error("Error when notifying update low watermark: {}", throwable, newLowWatermark);
                        }
                    });
        }
    }

    @Override
    public void setLowWatermarkOnRecovery(HybridTimestamp newLowWatermark) {
        setLowWatermark(newLowWatermark);
    }

    @Override
    public boolean tryLock(UUID lockId, HybridTimestamp lockTs) {
        updateLowWatermarkLock.readLock().lock();

        try {
            HybridTimestamp lwm = ts;
            if (lwm != null && lockTs.compareTo(lwm) < 0) {
                return false;
            }

            locks.put(lockId, new LowWatermarkLock(lockTs));

            return true;
        } finally {
            updateLowWatermarkLock.readLock().unlock();
        }
    }

    @Override
    public void unlock(UUID lockId) {
        LowWatermarkLock lock = locks.remove(lockId);

        if (lock == null) {
            // Already released.
            return;
        }

        lock.future().complete(null);
    }

    /**
     * Update low watermark and notify listeners.
     *
     * @param newTs New timestamp.
     * @return Listener notification future.
     */
    public CompletableFuture<Void> updateAndNotify(HybridTimestamp newTs) {
        try {
            assertNotNull(newTs);

            HybridTimestamp currentTs = ts;

            assertTrue(currentTs == null || currentTs.longValue() < newTs.longValue(), "ts=" + currentTs + ", newTs=" + newTs);

            return updateAndNotifyInternal(newTs);
        } catch (Throwable t) {
            return failedFuture(t);
        }
    }

    /** Set low watermark without listeners notification. */
    public void updateWithoutNotify(@Nullable HybridTimestamp newTs) {
        setLowWatermark(newTs);
    }

    private void setLowWatermark(@Nullable HybridTimestamp newTs) {
        updateLowWatermarkLock.writeLock().lock();

        try {
            ts = newTs;
        } finally {
            updateLowWatermarkLock.writeLock().unlock();
        }
    }

    private CompletableFuture<Void> updateAndNotifyInternal(HybridTimestamp newLowWatermark) {
        var parameters = new ChangeLowWatermarkEventParameters(newLowWatermark);

        return waitForLocksAndSetLowWatermark(newLowWatermark)
                .thenCompose(unused -> fireEvent(LOW_WATERMARK_CHANGED, parameters));
    }

    private CompletableFuture<Void> waitForLocksAndSetLowWatermark(HybridTimestamp newLowWatermark) {
        // Write lock so no new LWM locks can be added.
        updateLowWatermarkLock.writeLock().lock();

        try {
            for (LowWatermarkLock lock : locks.values()) {
                if (lock.timestamp().compareTo(newLowWatermark) <= 0) {
                    return lock.future().thenCompose(unused -> waitForLocksAndSetLowWatermark(newLowWatermark));
                }
            }

            setLowWatermark(newLowWatermark);

            return nullCompletedFuture();
        } finally {
            updateLowWatermarkLock.writeLock().unlock();
        }
    }
}
