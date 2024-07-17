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

import java.util.concurrent.CompletableFuture;

/**
 * Partition awareness provider for data streamer.
 *
 * @param <T> Item type.
 * @param <P> Partition type.
 */
public interface StreamerPartitionAwarenessProvider<T, P> {
    /**
     * Returns partition for item. This partition may or may not map to one or more actual Ignite table partitions.
     *
     * @param item Data item.
     * @return Partition.
     */
    P partition(T item);

    /**
     * Refreshes schemas and partition mapping asynchronously.
     *
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Void> refreshAsync();
}
