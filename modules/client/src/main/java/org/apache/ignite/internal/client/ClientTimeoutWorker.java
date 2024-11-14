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

import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.client.TcpClientChannel.TimeoutObjectImpl;
import org.apache.ignite.internal.future.timeout.TimeoutObject;
import org.apache.ignite.internal.future.timeout.TimeoutWorker;

final class ClientTimeoutWorker {
    public static final ClientTimeoutWorker INSTANCE = new ClientTimeoutWorker();

    private final Set<TcpClientChannel> channels = new ConcurrentHashMap<TcpClientChannel, Object>().keySet();

    void registerClientChannel(TcpClientChannel ch) {
        // TODO: Register channels
        // Loop in worker body, remove closed channels
        channels.add(ch);
    }

    private void body() {
        for (TcpClientChannel ch : channels) {
            if (ch.closed()) {
                channels.remove(ch);
            }

            for (Entry<Long, TimeoutObject> req : ch.pendingReqs.entrySet()) {
                // Check timeout.
                req.getValue().endTime();
            }
        }
    }
}
