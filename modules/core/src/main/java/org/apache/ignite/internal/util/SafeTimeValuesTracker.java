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

import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.util.Map;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * Safe time values tracker. Checks for reordering. Must be updated from a single thread.
 */
public class SafeTimeValuesTracker extends PendingComparableValuesTracker<HybridTimestamp, Void> {
    public SafeTimeValuesTracker(HybridTimestamp initialValue) {
        super(initialValue);
    }

    // Holds successful application context.
    private long commandIndex;
    private long commandTerm;
    private String cmdCls;

    /**
     * Update safe timestamp.
     *
     * @param safeTs The value.
     * @param commandIndex Command index.
     * @param commandTerm Command term.
     * @param cmdCls Command class name.
     */
    public void update(HybridTimestamp safeTs, long commandIndex, long commandTerm, String cmdCls) {
        if (!enterBusy()) {
            throw new TrackerClosedException();
        }

        try {
            Map.Entry<HybridTimestamp, @Nullable Void> current = this.current;

            IgniteBiTuple<HybridTimestamp, @Nullable Void> newEntry = new IgniteBiTuple<>(safeTs, null);

            // Entries from the same batch receive equal safe timestamps.
            if (comparator.compare(newEntry, current) < 0) {
                throw new IgniteInternalException(INTERNAL_ERR,
                        "Reordering detected: [old=" + current.getKey() + ", new=" + newEntry.get1() +
                                ", index=" + this.commandIndex +
                                ", term=" + this.commandTerm +
                                ", cmdCls=" + this.cmdCls +
                                ']');
            }

            CURRENT.set(this, newEntry);

            this.commandIndex = commandIndex;
            this.commandTerm = commandTerm;
            this.cmdCls = cmdCls;
        } finally {
            leaveBusy();
        }
    }
}
