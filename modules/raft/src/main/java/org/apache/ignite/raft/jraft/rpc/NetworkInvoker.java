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

package org.apache.ignite.raft.jraft.rpc;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RemotingException;
import org.jetbrains.annotations.Nullable;

/**
 * Rpc invocation.
 */
public interface NetworkInvoker {
    /**
     * Asynchronous invocation with a callback.
     *
     * @param peerId target peer ID
     * @param request request object
     * @param ctx invoke context
     * @param callback invoke callback
     * @param timeoutMs timeout millisecond
     *
     * @return The future.
     */
    CompletableFuture<Message> invokeAsync(
            PeerId peerId,
            Object request,
            @Nullable InvokeContext ctx,
            InvokeCallback callback,
            long timeoutMs
    ) throws InterruptedException, RemotingException;
}
