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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.function.Function;
import org.apache.ignite.compute.DeploymentUnit;
import org.jetbrains.annotations.Nullable;

/**
 * Represents an entity that can be used as a target for streaming data.
 *
 * @param <T> Entry type.
 */
public interface DataStreamerTarget<T> {
    /**
     * Streams data into the underlying table.
     *
     * @param publisher Producer.
     * @param options Options (can be null).
     * @return Future that will be completed when the stream is finished.
     */
    CompletableFuture<Void> streamData(
            Flow.Publisher<DataStreamerItem<T>> publisher,
            @Nullable DataStreamerOptions options);

    /**
     * Streams data with receiver. The receiver is responsible for processing the data and updating zero or more tables.
     *
     * @param publisher Producer.
     * @param options Options (can be null).
     * @param keyFunc Key function. The key is only used locally for colocation.
     * @param payloadFunc Payload function. The payload is sent to the receiver.
     * @param resultSubscriber Optional subscriber for the receiver results.
     *     NOTE: The result subscriber follows the pace of publisher and ignores backpressure
     *     from {@link Flow.Subscription#request(long)} calls.
     * @param deploymentUnits Target deployment units. Can be empty.
     * @param receiverClassName Receiver class name.
     * @param receiverArgs Receiver arguments.
     * @return Future that will be completed when the stream is finished.
     * @param <E> Producer item type.
     * @param <V> Payload type.
     * @param <R> Result type.
     */
    <E, V, R> CompletableFuture<Void> streamData(
            Flow.Publisher<E> publisher,
            @Nullable DataStreamerOptions options,
            Function<E, T> keyFunc,
            Function<E, V> payloadFunc,
            @Nullable Flow.Subscriber<R> resultSubscriber,
            List<DeploymentUnit> deploymentUnits,
            String receiverClassName,
            Object... receiverArgs);
}
