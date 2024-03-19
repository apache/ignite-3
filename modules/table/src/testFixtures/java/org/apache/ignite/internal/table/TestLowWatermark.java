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

package org.apache.ignite.internal.table;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.table.distributed.LowWatermark;
import org.apache.ignite.internal.table.distributed.LowWatermarkChangedListener;
import org.jetbrains.annotations.Nullable;

/**
 * Low watermark dummy implementation, which requires explicit {@link #updateAndNotify(HybridTimestamp)} method call to notify listeners.
 * This implementation has no persistent state and notifies listeners instantly in same thread.
 */
public class TestLowWatermark implements LowWatermark {
    private final List<LowWatermarkChangedListener> listeners = new CopyOnWriteArrayList<>();

    private volatile @Nullable HybridTimestamp ts;

    private final ReadWriteLock updateLowWatermarkLock = new ReentrantReadWriteLock();

    @Override
    public @Nullable HybridTimestamp getLowWatermark() {
        return ts;
    }

    @Override
    public void addUpdateListener(LowWatermarkChangedListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeUpdateListener(LowWatermarkChangedListener listener) {
        listeners.remove(listener);
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

    /**
     * Update low watermark and notify listeners.
     *
     * @param newTs New timestamp.
     * @return Listener notification future.
     */
    public CompletableFuture<Void> updateAndNotify(HybridTimestamp newTs) {
        try {
            assertNotNull(newTs);

            assertTrue(ts == null || ts.longValue() < newTs.longValue(), "ts=" + ts + ", newTs=" + newTs);

            setLowWatermark(newTs);

            return allOf(listeners.stream().map(l -> l.onLwmChanged(newTs)).toArray(CompletableFuture[]::new));
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
}
