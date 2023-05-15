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

/**
 * Stream receiver.
 *
 * <p>See {@link StreamerTarget#streamData}.</p>
 *
 * @param <T> Element type.
 */
@FunctionalInterface
public interface StreamReceiver<T, R> {
    /**
     * Receive a data item and return a result.
     * This method is called on server node for every data item.
     *
     * @param item Data item.
     * @param context Stream receiver context.
     * @return Future that is completed when the stream is processed. The result of the future, when not null,
     * is passed back to the resultSubscriber on the client.
     */
    CompletableFuture<R> receive(T item, StreamReceiverContext context);
}
