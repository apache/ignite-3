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

package org.apache.ignite.internal.streamer;

/**
 * Streamer options.
 */
public interface StreamerOptions {
    /**
     * Gets the page size (the number of entries that will be sent to the cluster in one network call).
     *
     * @return Batch size.
     */
    int pageSize();

    /**
     * Gets the number of parallel operations per partition (how many in-flight requests can be active for a given partition).
     *
     * @return Per node parallel operations.
     */
    int perPartitionParallelOperations();

    /**
     * Gets the auto flush interval, in milliseconds
     * (the period of time after which the streamer will flush the per-node buffer even if it is not full).
     *
     * @return Auto flush interval.
     */
    int autoFlushInterval();
}
