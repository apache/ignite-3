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

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Data streamer target.
 *
 * @param <R> Record type.
 */
public interface StreamerTarget<R> {
    // TODO: Can we have async producer AND async consumer? Like IAsyncEnumerable in .NET?
    // What if I want to have multiple producers threads on the client - it is not possible with Stream?
    CompletableFuture<Void> streamData(Stream<R> data); // Sync producer, async consumer

    /**
     * Stream data to the cluster with a receiver.
     *
     * @param data Producer stream.
     * @param keyAccessor Key accessor. Required to determine target node from the entry key.
     * @param receiver Stream receiver. Will be invoked on the target node.
     * @return Future that will be completed when the stream is finished.
     * @param <T> Entry type.
     */
    <T> CompletableFuture<Void> streamData(Stream<T> data, Function<T, R> keyAccessor, StreamReceiver<T> receiver);
}
