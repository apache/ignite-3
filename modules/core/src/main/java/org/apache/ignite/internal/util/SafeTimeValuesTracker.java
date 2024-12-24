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

package org.apache.ignite.internal.util;

import java.util.Map;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Safe time values tracker. Checks for reordering. Must be updated from a single thread.
 */
public class SafeTimeValuesTracker extends PendingComparableValuesTracker<HybridTimestamp, Void> {
    public SafeTimeValuesTracker(HybridTimestamp initialValue) {
        super(initialValue);
    }

    @Override
    public void update(HybridTimestamp newValue, @Nullable Void futureResult) {
        if (!enterBusy()) {
            throw new TrackerClosedException();
        }

        try {
            Map.Entry<HybridTimestamp, @Nullable Void> current = this.current;

            IgniteBiTuple<HybridTimestamp, @Nullable Void> newEntry = new IgniteBiTuple<>(newValue, futureResult);

            // Entries from the same batch receive equal safe timestamps.
            if (comparator.compare(newEntry, current) < 0) {
                throw new AssertionError("Reordering detected: [old=" + current.getKey() + ", new=" + newEntry.get1() + ']');
            }

            CURRENT.set(this, newEntry);
            completeWaitersOnUpdate(newValue, futureResult);
        } finally {
            leaveBusy();
        }
    }
}
