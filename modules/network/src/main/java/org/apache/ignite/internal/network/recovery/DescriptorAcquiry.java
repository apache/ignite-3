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

package org.apache.ignite.internal.network.recovery;

import io.netty.channel.Channel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.ignite.internal.network.netty.NettySender;

/**
 * Context around a fact that a {@link RecoveryDescriptor} is acquired by some channel.
 */
class DescriptorAcquiry {
    private final Channel channel;
    private final CompletableFuture<NettySender> handshakeCompleteFuture;

    private final CompletableFuture<Void> clinchResolved = new CompletableFuture<>();

    DescriptorAcquiry(Channel channel, CompletableFuture<NettySender> handshakeCompleteFuture) {
        this.channel = channel;
        this.handshakeCompleteFuture = handshakeCompleteFuture;
    }

    /**
     * Returns the channel that owns the descriptor.
     */
    Channel channel() {
        return channel;
    }

    /**
     * Returns a completion stage that gets completed when a clinch associated with this acquiry is resolved
     * (that is, the owning handshake gave up and released the recovery descriptor).
     */
    CompletionStage<Void> clinchResolved() {
        return clinchResolved;
    }

    /**
     * Signals that the owner of this recovery descriptor gave up and, hence, the clinch has been resolved.
     */
    void markClinchResolved() {
        clinchResolved.complete(null);
    }

    /**
     * Returns the future that gets completed when the handshake performed by the owner of the descriptor completes.
     */
    CompletableFuture<NettySender> handshakeCompleteFuture() {
        return handshakeCompleteFuture;
    }
}
