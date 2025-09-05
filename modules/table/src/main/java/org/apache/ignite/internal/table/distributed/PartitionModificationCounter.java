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

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.jetbrains.annotations.Nullable;

/**
 * Partition modification counter.
 */
public interface PartitionModificationCounter {
    /**
     * Adds the given value to the current counter value.
     *
     * @param delta the value to add.
     * @param commitTimestamp the commit timestamp of the transaction that made the modification.
     */
    void updateValue(int delta, HybridTimestamp commitTimestamp);

    /**
     * Gets the current counter value.
     *
     * @return the current counter value
     */
    long value();

    /**
     * Returns a timestamp representing the commit time of the
     * last transaction that caused the counter to reach a milestone.
     *
     * @return Timestamp of last milestone reached.
     */
    HybridTimestamp lastMilestoneTimestamp();

    /** No-op modification counter. */
    PartitionModificationCounter NOOP = new PartitionModificationCounter() {
        @Override
        public void updateValue(int delta, HybridTimestamp commitTimestamp) {
            // No-op.
        }

        @Override
        public long value() {
            return -1;
        }

        @Override
        public @Nullable HybridTimestamp lastMilestoneTimestamp() {
            return null;
        }
    };
}
