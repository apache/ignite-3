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

package org.apache.ignite.internal.metastorage.server.persistence;

import java.util.concurrent.TimeUnit;

/**
 * Helper class to hold compaction statistics while it's running, and then to get the message with accumulated statistics.
 */
class CompactionStatisticsHolder {
    /** Compaction revision. */
    private final long revision;
    /** {@link System#nanoTime()} at the moment of compaction start. */
    private final long startNanoTime;
    /** {@link System#nanoTime()} at the moment of compaction completion. */
    private long finishNanoTime;

    /** Whether compaction has been stopped in the process. */
    private boolean cancelled;

    /** Total number of keys read from rocksb iterator.*/
    private int totalKeysScanned;
    /** Total number of keys read from metastorage, this metric excludes duplicates.*/
    private int totalKeysRead;

    /** Total number of individual revisions for all keys that were compacted. */
    private int totalKeyRevisionsCompacted;
    /** Total number of keys that had at least one revision compacted. */
    private int totalKeysCompacted;
    /** Total number compacted tombstones. */
    private int totalTombstonesCompacted;

    /** Total number of committed batches. */
    private int batchesCommitted;
    /** Total number of retries while trying to commit a batch. */
    private int batchesAborted;

    /** {@link #totalKeysRead} accumulator for current batch. */
    private int batchKeysRead;

    /** {@link #totalKeyRevisionsCompacted} accumulator for current batch. */
    private int batchKeyRevisionsCompacted;
    /** {@link #totalKeysCompacted} accumulator for current batch. */
    private int batchKeysCompacted;
    /** {@link #totalTombstonesCompacted} accumulator for current batch. */
    private int batchTombstonesCompacted;

    /** Total number of auxiliary mappings compacted. Matches a total number of compacted revisions. */
    private int auxiliaryMappingsCompacted;

    /** Temporary value for calculating {@link #maxLockWaitNanos} and {@link #maxLockHoldNanos}. */
    private long lastLockNanoTime;
    /** Max time of lock wait acquisition, in nanoseconds. */
    private long maxLockWaitNanos;
    /** Max time of lock holding, in nanoseconds. */
    private long maxLockHoldNanos;

    CompactionStatisticsHolder(long revision) {
        this.revision = revision;
        this.startNanoTime = System.nanoTime();
    }

    String info() {
        return "cancelled=" + cancelled
                + ", compactedRevision=" + revision
                + ", duration=" + TimeUnit.NANOSECONDS.toMillis(finishNanoTime - startNanoTime) + "ms"
                + ", batchesCommitted=" + batchesCommitted
                + ", batchesAborted=" + batchesAborted
                + ", keysScanned=" + totalKeysScanned
                + ", keysRead=" + totalKeysRead
                + ", keyRevisionsCompacted=" + totalKeyRevisionsCompacted
                + ", keysCompacted=" + totalKeysCompacted
                + ", tombstonesCompacted=" + totalTombstonesCompacted
                + ", auxiliaryMappingsCompacted=" + auxiliaryMappingsCompacted
                + ", maxLockWait=" + TimeUnit.NANOSECONDS.toMicros(maxLockWaitNanos) + "us"
                + ", maxLockHold=" + TimeUnit.NANOSECONDS.toMicros(maxLockHoldNanos) + "us";
    }

    void onCancelled() {
        cancelled = true;
    }

    void onFinished() {
        finishNanoTime = System.nanoTime();
    }

    void onBatchCommitted() {
        batchesCommitted++;

        totalKeysRead += batchKeysRead;
        totalKeyRevisionsCompacted += batchKeyRevisionsCompacted;
        totalKeysCompacted += batchKeysCompacted;
        totalTombstonesCompacted += batchTombstonesCompacted;

        resetBatch();
    }

    void onBatchAborted() {
        batchesAborted++;

        resetBatch();
    }

    void onKeyEncountered() {
        totalKeysScanned++;
        batchKeysRead++;
    }

    void onKeyRevisionCompacted() {
        batchKeyRevisionsCompacted++;
    }

    void onTombstoneCompacted() {
        batchTombstonesCompacted++;
    }

    void onKeyCompacted() {
        batchKeysCompacted++;
    }

    void onAuxiliaryMappingCompacted() {
        auxiliaryMappingsCompacted++;
    }

    void onBeforeWriteBatchLock() {
        lastLockNanoTime = System.nanoTime();
    }

    void onAfterWriteBatchLock() {
        maxLockWaitNanos = Math.max(maxLockWaitNanos, System.nanoTime() - lastLockNanoTime);

        lastLockNanoTime = System.nanoTime();
    }

    private void resetBatch() {
        batchKeysRead = 0;
        batchKeyRevisionsCompacted = 0;
        batchKeysCompacted = 0;
        batchTombstonesCompacted = 0;

        maxLockHoldNanos = Math.max(maxLockHoldNanos, System.nanoTime() - lastLockNanoTime);
    }
}
