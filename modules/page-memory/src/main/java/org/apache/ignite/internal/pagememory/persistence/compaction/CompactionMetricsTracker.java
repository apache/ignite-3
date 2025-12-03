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

package org.apache.ignite.internal.pagememory.persistence.compaction;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Tracks various compaction phases and stats.
 */
public class CompactionMetricsTracker {
    private static final AtomicIntegerFieldUpdater<CompactionMetricsTracker> DATA_PAGES_WRITTEN_UPDATER =
            newUpdater(CompactionMetricsTracker.class, "dataPagesWritten");

    private static final AtomicIntegerFieldUpdater<CompactionMetricsTracker> DATA_PAGES_SKIPPED_UPDATER =
            newUpdater(CompactionMetricsTracker.class, "dataPagesSkipped");

    private volatile int dataPagesWritten;

    private volatile int dataPagesSkipped;

    private final long startNanos = System.nanoTime();

    private long endNanos;

    /**
     * Increments counter if data page was written.
     *
     * <p>Thread safe.
     */
    public void onDataPageWritten() {
        DATA_PAGES_WRITTEN_UPDATER.incrementAndGet(this);
    }

    /**
     * Increments counter if data page was skipped.
     *
     * <p>Thread safe.
     */
    public void onPageSkipped() {
        DATA_PAGES_SKIPPED_UPDATER.incrementAndGet(this);
    }

    /**
     * Callback on compaction end.
     *
     * <p>Not thread safe.
     */
    public void onCompactionEnd() {
        endNanos = System.nanoTime();
    }

    /**
     * Returns data pages written.
     *
     * <p>Thread safe.
     */
    public int dataPagesWritten() {
        return dataPagesWritten;
    }

    /**
     * Returns data pages skipped.
     *
     * <p>Thread safe.
     */
    public int dataPagesSkipped() {
        return dataPagesSkipped;
    }

    /**
     * Returns total compaction duration.
     *
     * <p>Not thread safe.
     */
    public long totalDuration(TimeUnit timeUnit) {
        return timeUnit.convert(endNanos - startNanos, NANOSECONDS);
    }
}
