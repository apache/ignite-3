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

package org.apache.ignite.table;

/**
 * Data streamer options. See {@link DataStreamerTarget} for more information.
 */
public class DataStreamerOptions {
    /** Default options. */
    public static final DataStreamerOptions DEFAULT = builder().build();

    private final int batchSize;

    private final int perNodeParallelOperations;

    private final int autoFlushFrequency;

    /**
     * Constructor.
     *
     * @param batchSize Batch size.
     * @param perNodeParallelOperations Per node parallel operations.
     * @param autoFlushFrequency Auto flush frequency.
     */
    private DataStreamerOptions(int batchSize, int perNodeParallelOperations, int autoFlushFrequency) {
        this.batchSize = batchSize;
        this.perNodeParallelOperations = perNodeParallelOperations;
        this.autoFlushFrequency = autoFlushFrequency;
    }

    /**
     * Creates a new builder.
     *
     * @return Builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Gets the batch size (the number of entries that will be sent to the cluster in one network call).
     *
     * @return Batch size.
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * Gets the number of parallel operations per node (how many in-flight requests can be active for a given node).
     *
     * @return Per node parallel operations.
     */
    public int perNodeParallelOperations() {
        return perNodeParallelOperations;
    }

    /**
     * Gets the auto flush frequency, in milliseconds
     * (the period of time after which the streamer will flush the per-node buffer even if it is not full).
     *
     * @return Auto flush frequency.
     */
    public int autoFlushFrequency() {
        return autoFlushFrequency;
    }

    /**
     * Builder.
     */
    static class Builder {
        private int batchSize = 1000;

        private int perNodeParallelOperations = 4;

        private int autoFlushFrequency = 5000;

        /**
         * Sets the batch size (the number of entries that will be sent to the cluster in one network call).
         *
         * @param batchSize Batch size.
         * @return This builder instance.
         */
        public Builder batchSize(int batchSize) {
            if (batchSize <= 0) {
                throw new IllegalArgumentException("Batch size must be positive: " + batchSize);
            }

            this.batchSize = batchSize;

            return this;
        }

        /**
         * Sets the number of parallel operations per node (how many in-flight requests can be active for a given node).
         *
         * @param perNodeParallelOperations Per node parallel operations.
         * @return This builder instance.
         */
        public Builder perNodeParallelOperations(int perNodeParallelOperations) {
            if (perNodeParallelOperations <= 0) {
                throw new IllegalArgumentException("Per node parallel operations must be positive: " + perNodeParallelOperations);
            }

            this.perNodeParallelOperations = perNodeParallelOperations;

            return this;
        }

        /**
         * Sets the auto flush frequency, in milliseconds
         * (the period of time after which the streamer will flush the per-node buffer even if it is not full).
         *
         * @param autoFlushFrequency Auto flush frequency, in milliseconds. 0 or less means no auto flush.
         * @return This builder instance.
         */
        public Builder autoFlushFrequency(int autoFlushFrequency) {
            this.autoFlushFrequency = autoFlushFrequency;

            return this;
        }

        /**
         * Builds the options.
         *
         * @return Data streamer options.
         */
        public DataStreamerOptions build() {
            return new DataStreamerOptions(batchSize, perNodeParallelOperations, autoFlushFrequency);
        }
    }
}
