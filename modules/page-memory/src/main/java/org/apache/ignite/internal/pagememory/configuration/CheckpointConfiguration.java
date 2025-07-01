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

package org.apache.ignite.internal.pagememory.configuration;

import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

/** Checkpoint configuration. */
public class CheckpointConfiguration {
    /** Default value for {@link #checkpointThreads()}. */
    public static final int DEFAULT_CHECKPOINT_THREADS = 4;

    /** Default value for {@link #compactionThreads()}. */
    public static final int DEFAULT_COMPACTION_THREADS = 4;

    /** Default value for {@link #intervalMillis()}. */
    public static final long DEFAULT_CHECKPOINT_INTERVAL = 180_000L;

    /** Default value for {@link #intervalDeviationPercent()}. */
    public static final int DEFAULT_CHECKPOINT_INTERVAL_DEVIATION = 40;

    /** Default value for {@link #readLockTimeoutMillis()}. */
    public static final int DEFAULT_CHECKPOINT_READ_LOCK_TIMEOUT = 10_000;

    /** Default value for {@link #logReadLockThresholdTimeoutMillis()}. */
    public static final int DEFAULT_CHECKPOINT_LOG_READ_LOCK_THRESHOLD_TIMEOUT = 0;

    private final int checkpointThreads;
    private final int compactionThreads;

    private final LongSupplier intervalMillis;
    private final IntSupplier intervalDeviationPercent;

    private final LongSupplier readLockTimeoutMillis;
    private final LongSupplier logReadLockThresholdTimeoutMillis;

    private CheckpointConfiguration(
            int checkpointThreads,
            int compactionThreads,
            LongSupplier intervalMillis,
            IntSupplier intervalDeviationPercent,
            LongSupplier readLockTimeoutMillis,
            LongSupplier logReadLockThresholdTimeoutMillis
    ) {
        this.checkpointThreads = checkpointThreads;
        this.compactionThreads = compactionThreads;
        this.intervalMillis = intervalMillis;
        this.intervalDeviationPercent = intervalDeviationPercent;
        this.readLockTimeoutMillis = readLockTimeoutMillis;
        this.logReadLockThresholdTimeoutMillis = logReadLockThresholdTimeoutMillis;
    }

    /** Number of checkpoint threads. */
    public int checkpointThreads() {
        return checkpointThreads;
    }

    /** Number of threads to compact delta files. */
    public int compactionThreads() {
        return compactionThreads;
    }

    /** Interval between checkpoints in milliseconds. */
    public long intervalMillis() {
        return intervalMillis.getAsLong();
    }

    /**
     * Max deviation (in percent) of intervals between checkpoints. If this is 20 and {@link #intervalMillis()} is 1000, then the effective
     * checkpoint interval values will be between 900 and 1100.
     */
    public int intervalDeviationPercent() {
        return intervalDeviationPercent.getAsInt();
    }

    /** Timeout for checkpoint read lock acquisition in milliseconds. */
    public long readLockTimeoutMillis() {
        return readLockTimeoutMillis.getAsLong();
    }

    /** Threshold for logging (if greater than zero) read lock holders in milliseconds. */
    public long logReadLockThresholdTimeoutMillis() {
        return logReadLockThresholdTimeoutMillis.getAsLong();
    }

    /** Creates a builder for {@link CheckpointConfiguration} instance. */
    public static CheckpointConfigurationBuilder builder() {
        return new CheckpointConfigurationBuilder();
    }

    /** Builder for {@link CheckpointConfiguration}. */
    public static final class CheckpointConfigurationBuilder {
        private int checkpointThreads = DEFAULT_CHECKPOINT_THREADS;
        private int compactionThreads = DEFAULT_COMPACTION_THREADS;

        private LongSupplier intervalMillis = () -> DEFAULT_CHECKPOINT_INTERVAL;
        private IntSupplier intervalDeviationPercent = () -> DEFAULT_CHECKPOINT_INTERVAL_DEVIATION;

        private LongSupplier readLockTimeoutMillis = () -> DEFAULT_CHECKPOINT_READ_LOCK_TIMEOUT;
        private LongSupplier logReadLockThresholdTimeoutMillis = () -> DEFAULT_CHECKPOINT_LOG_READ_LOCK_THRESHOLD_TIMEOUT;

        CheckpointConfigurationBuilder() {
        }

        public CheckpointConfigurationBuilder checkpointThreads(int checkpointThreads) {
            this.checkpointThreads = checkpointThreads;
            return this;
        }

        public CheckpointConfigurationBuilder compactionThreads(int compactionThreads) {
            this.compactionThreads = compactionThreads;
            return this;
        }

        public CheckpointConfigurationBuilder intervalMillis(LongSupplier intervalMillis) {
            this.intervalMillis = intervalMillis;
            return this;
        }

        public CheckpointConfigurationBuilder intervalDeviationPercent(IntSupplier intervalDeviationPercent) {
            this.intervalDeviationPercent = intervalDeviationPercent;
            return this;
        }

        public CheckpointConfigurationBuilder readLockTimeoutMillis(LongSupplier readLockTimeoutMillis) {
            this.readLockTimeoutMillis = readLockTimeoutMillis;
            return this;
        }

        public CheckpointConfigurationBuilder logReadLockThresholdTimeoutMillis(LongSupplier logReadLockThresholdTimeoutMillis) {
            this.logReadLockThresholdTimeoutMillis = logReadLockThresholdTimeoutMillis;
            return this;
        }

        /** Builds a {@link CheckpointConfiguration} instance. */
        public CheckpointConfiguration build() {
            return new CheckpointConfiguration(
                    checkpointThreads,
                    compactionThreads,
                    intervalMillis,
                    intervalDeviationPercent,
                    readLockTimeoutMillis,
                    logReadLockThresholdTimeoutMillis
            );
        }
    }
}
