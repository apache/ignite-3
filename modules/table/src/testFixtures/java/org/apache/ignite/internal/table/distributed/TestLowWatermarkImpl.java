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

import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.jetbrains.annotations.Nullable;

/** Test implementation. */
public class TestLowWatermarkImpl implements LowWatermark {
    private volatile HybridTimestamp lowWatermark;

    private final ReadWriteLock updateLowWatermarkLock = new ReentrantReadWriteLock();

    private final List<LowWatermarkChangedListener> updateListeners = new CopyOnWriteArrayList<>();

    @Override
    public @Nullable HybridTimestamp getLowWatermark() {
        return lowWatermark;
    }

    @Override
    public void getLowWatermarkSafe(Consumer<@Nullable HybridTimestamp> consumer) {
        updateLowWatermarkLock.readLock().lock();

        try {
            consumer.accept(lowWatermark);
        } finally {
            updateLowWatermarkLock.readLock().unlock();
        }
    }

    @Override
    public void addUpdateListener(LowWatermarkChangedListener listener) {
        updateListeners.add(listener);
    }

    @Override
    public void removeUpdateListener(LowWatermarkChangedListener listener) {
        updateListeners.remove(listener);
    }

    /** Sets a low watermark and notifies listeners. */
    public CompletableFuture<Void> setLowWatermark(HybridTimestamp newLowWatermark) {
        assertNotNull(newLowWatermark);

        updateLowWatermarkLock.writeLock().lock();

        try {
            lowWatermark = newLowWatermark;

            return notifyListeners(newLowWatermark);
        } finally {
            updateLowWatermarkLock.writeLock().unlock();
        }
    }

    private CompletableFuture<Void> notifyListeners(HybridTimestamp newLowWatermark) {
        if (updateListeners.isEmpty()) {
            return nullCompletedFuture();
        }

        CompletableFuture<?>[] futures = updateListeners.stream()
                .map(listener -> listener.onLwmChanged(newLowWatermark))
                .toArray(CompletableFuture[]::new);

        return allOf(futures);
    }
}
