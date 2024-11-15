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

package org.apache.ignite.internal.client;

import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;

import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.future.timeout.TimeoutObject;

final class ClientTimeoutWorker {
    static final ClientTimeoutWorker INSTANCE = new ClientTimeoutWorker();

    private final Set<TcpClientChannel> channels = new ConcurrentHashMap<TcpClientChannel, Object>().keySet();

    void registerClientChannel(TcpClientChannel ch) {
        // TODO: Stop thread when idle.
        channels.add(ch);
    }

    private void checkTimeouts() {
        long now = coarseCurrentTimeMillis();

        for (TcpClientChannel ch : channels) {
            if (ch.closed()) {
                channels.remove(ch);
            }

            for (Entry<Long, TimeoutObject<CompletableFuture<ClientMessageUnpacker>>> req : ch.pendingReqs.entrySet()) {
                TimeoutObject<CompletableFuture<ClientMessageUnpacker>> timeoutObject = req.getValue();

                if (timeoutObject != null && timeoutObject.endTime() > 0 && now > timeoutObject.endTime()) {
                    // Client-facing future will fail with a timeout, but internal ClientRequestFuture will stay in the map -
                    // otherwise we'll fail with "protocol breakdown" error when a late response arrives from the server.
                    CompletableFuture<?> fut = timeoutObject.future();
                    fut.completeExceptionally(new TimeoutException());
                }
            }
        }
    }
}
