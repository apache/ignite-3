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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
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

    @Override
    public @Nullable HybridTimestamp getLowWatermark() {
        return ts;
    }

    @Override
    public void addUpdateListener(LowWatermarkChangedListener listener) {
        this.listeners.add(listener);
    }

    @Override
    public void removeUpdateListener(LowWatermarkChangedListener listener) {
        this.listeners.remove(listener);
    }

    @Override
    public void getLowWatermarkSafe(Consumer<HybridTimestamp> consumer) {
        consumer.accept(ts);
    }

    /**
     * Update low watermark and notify listeners.
     *
     * @param newTs New timestamp.
     * @return Listener notification future.
     */
    public CompletableFuture<Void> updateAndNotify(HybridTimestamp newTs) {
        assert newTs != null;
        assert ts == null || ts.longValue() < newTs.longValue() : "ts=" + ts + ", newTs=" + newTs;

        this.ts = newTs;

        return CompletableFuture.allOf(listeners.stream().map(l -> l.onLwmChanged(newTs)).toArray(CompletableFuture[]::new));
    }

    /** Set low watermark without listeners notification. */
    public void update(HybridTimestamp newTs) {
        this.ts = newTs;
    }
}
