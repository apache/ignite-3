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

package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * Tracks write statistics for a partition during checkpoint, including total writes
 * and whether the main file received writes. Delta file writes are always assumed
 * to occur in the current system.
 *
 * <p>This class is thread-safe for use by multiple checkpoint writer threads.
 */
class PartitionWriteStats {
    /** Total number of writes to this partition. */
    private final LongAdder totalWrites = new LongAdder();

    /** Whether the main file received any writes. */
    private final AtomicBoolean hasMainFileWrites = new AtomicBoolean(false);

    /**
     * Records a page write.
     */
    void recordWrite() {
        totalWrites.increment();
    }

    /**
     * Records that the main file received a write.
     */
    void recordMainFileWrite() {
        hasMainFileWrites.set(true);
    }

    /**
     * Returns the total number of writes to this partition.
     */
    int getTotalWrites() {
        return totalWrites.intValue();
    }

    /**
     * Returns whether the main file received any writes.
     */
    boolean hasMainFileWrites() {
        return hasMainFileWrites.get();
    }
}
