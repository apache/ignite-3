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

    private final int pageSize;

    private final int perPartitionParallelOperations;

    private final int autoFlushFrequency;

    private final int retryLimit;

    /**
     * Constructor.
     *
     * @param pageSize Page size.
     * @param perPartitionParallelOperations Per partition parallel operations.
     * @param autoFlushFrequency Auto flush frequency.
     * @param retryLimit Retry limit.
     */
    private DataStreamerOptions(int pageSize, int perPartitionParallelOperations, int autoFlushFrequency, int retryLimit) {
        this.pageSize = pageSize;
        this.perPartitionParallelOperations = perPartitionParallelOperations;
        this.autoFlushFrequency = autoFlushFrequency;
        this.retryLimit = retryLimit;
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
     * Gets the page size (the number of entries that will be sent to the cluster in one network call).
     *
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * Gets the number of parallel operations per partition (how many in-flight requests can be active for a given partition).
     *
     * @return Per node parallel operations.
     */
    public int perPartitionParallelOperations() {
        return perPartitionParallelOperations;
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
     * Gets the retry limit for a page. If a page fails to be sent to the cluster, the streamer will retry it a number of times.
     * If all retries fail, the streamer will be aborted.
     *
     * @return Retry limit.
     */
    public int retryLimit() {
        return retryLimit;
    }

    /**
     * Builder.
     */
    public static class Builder {
        private int pageSize = 1000;

        private int perPartitionParallelOperations = 1;

        private int autoFlushFrequency = 5000;

        private int retryLimit = 16;

        /**
         * Sets the page size (the number of entries that will be sent to the cluster in one network call).
         *
         * @param pageSize Page size.
         * @return This builder instance.
         */
        public Builder pageSize(int pageSize) {
            if (pageSize <= 0) {
                throw new IllegalArgumentException("Page size must be positive: " + pageSize);
            }

            this.pageSize = pageSize;

            return this;
        }

        /**
         * Sets the number of parallel operations per partition (how many in-flight requests can be active for a given partition).
         *
         * @param perPartitionParallelOperations Per partition parallel operations.
         * @return This builder instance.
         */
        public Builder perPartitionParallelOperations(int perPartitionParallelOperations) {
            if (perPartitionParallelOperations <= 0) {
                throw new IllegalArgumentException("perPartitionParallelOperations must be positive: " + perPartitionParallelOperations);
            }

            this.perPartitionParallelOperations = perPartitionParallelOperations;

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
         * Sets the retry limit for a page. If a page fails to be sent to the cluster, the streamer will retry it a number of times.
         * If all retries fail, the streamer will be aborted.
         *
         * @param retryLimit Retry limit.
         * @return This builder instance.
         */
        public Builder retryLimit(int retryLimit) {
            this.retryLimit = retryLimit;

            return this;
        }

        /**
         * Builds the options.
         *
         * @return Data streamer options.
         */
        public DataStreamerOptions build() {
            return new DataStreamerOptions(pageSize, perPartitionParallelOperations, autoFlushFrequency, retryLimit);
        }
    }
}
