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

import java.util.BitSet;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Streamer batch sender.
 *
 * @param <T> Item type.
 * @param <P> Partition type.
 */
@FunctionalInterface
public interface StreamerBatchSender<T, P> {
    /**
     * Sends batch of items asynchronously.
     *
     * @param partition Partition.
     * @param batch Batch.
     * @param deleted Deleted items (one bit per row in the batch).
     * @return Future representing pending completion of the operation.
     */
    CompletableFuture<Void> sendAsync(P partition, Collection<T> batch, @Nullable BitSet deleted);
}
