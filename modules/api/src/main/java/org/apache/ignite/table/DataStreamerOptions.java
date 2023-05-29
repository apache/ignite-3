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
     * Gets the batch size. The batch size is the number of entries that will be sent to the cluster in one network call.
     *
     * @return Batch size.
     */
    public int batchSize() {
        return batchSize;
    }

    public int perNodeParallelOperations() {
        return perNodeParallelOperations;
    }

    public int autoFlushFrequency() {
        return autoFlushFrequency;
    }

    static class Builder {
        private int batchSize = 1000;

        private int perNodeParallelOperations = 4;

        private int autoFlushFrequency = 5000;

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;

            return this;
        }

        public Builder perNodeParallelOperations(int perNodeParallelOperations) {
            this.perNodeParallelOperations = perNodeParallelOperations;

            return this;
        }

        public Builder autoFlushFrequency(int autoFlushFrequencyMs) {
            this.autoFlushFrequency = autoFlushFrequencyMs;

            return this;
        }

        public DataStreamerOptions build() {
            return new DataStreamerOptions(batchSize, perNodeParallelOperations, autoFlushFrequency);
        }
    }
}
