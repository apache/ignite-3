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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.future.timeout.TimeoutWorker;

final class ClientTimeoutWorker {
    public static final ClientTimeoutWorker INSTANCE = new ClientTimeoutWorker();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private final ArrayList<TcpClientChannel> channels = new ArrayList<>();

    void registerClientChannel(TcpClientChannel ch) {
        // TODO: Register channels
        // Loop in worker body, remove closed channels

        rwLock.writeLock().lock();

        try {
            channels.add(ch);
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
