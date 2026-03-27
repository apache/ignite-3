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

package org.apache.ignite.client.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelOutboundInvoker;
import java.util.concurrent.locks.ReentrantLock;

class ResponseWriteGuard {
    private final ReentrantLock lock = new ReentrantLock();

    private boolean responseWritten = false;

    boolean write(ChannelOutboundInvoker ctx, ByteBuf buf) {
        // No double-check, contention or false case is extremely rare.
        lock.lock();
        try {
            if (responseWritten) {
                return false;
            }

            ctx.write(buf);
            responseWritten = true;

            return true;
        } finally {
            lock.unlock();
        }
    }
}
