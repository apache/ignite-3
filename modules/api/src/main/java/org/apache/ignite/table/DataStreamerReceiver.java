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
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

/**
 * Data streamer receiver.
 *
 * @param <T> Payload type.
 * @param <R> Result type.
 */
@SuppressWarnings("InterfaceMayBeAnnotatedFunctional")
public interface DataStreamerReceiver<T, R> {
    /**
     * Receives an item from the data streamer (see {@link DataStreamerTarget#streamData(Publisher, DataStreamerOptions,
     * Function, Function, Subscriber, List, String, Object...)}).
     *
     * <p>The receiver is called for each page (batch) in the data streamer and is responsible for processing the items,
     * updating zero or more tables, and returning a result.
     *
     * @param page Item batch.
     * @param ctx Receiver context.
     * @param args Additional arguments.
     * @return Future with the result. Null future for synchronous completion.
     */
    @Nullable CompletableFuture<List<R>> receive(
            List<T> page,
            DataStreamerReceiverContext ctx,
            Object... args);
}