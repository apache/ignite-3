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

package org.apache.ignite.internal.raft;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import org.apache.ignite.internal.network.NetworkMessage;

/**
 * Represents a context containing data for {@code RaftGroupServiceImpl#sendWithRetry} methods.
 *
 * <p>Not thread-safe.
 */
class RetryContext {
    private Peer targetPeer;

    private final Function<Peer, ? extends NetworkMessage> requestFactory;

    private NetworkMessage request;

    private final long stopTime;

    private int retryCount;

    private final Set<Peer> unavailablePeers = new HashSet<>();

    /**
     * Creates a context.
     *
     * @param targetPeer Target peer to send the request to.
     * @param requestFactory Factory for creating requests to the target peer.
     * @param stopTime Timestamp that denotes the point in time up to which retry attempts will be made.
     * @param retryCount Number of retries made. sendWithRetry method has a recursion nature, in case of recoverable exceptions or peer
     *     unavailability it'll be scheduled for a next attempt. Generally a request will be retried until success or timeout.
     */
    RetryContext(Peer targetPeer, Function<Peer, ? extends NetworkMessage> requestFactory, long stopTime, int retryCount) {
        this.targetPeer = targetPeer;
        this.requestFactory = requestFactory;
        this.request = requestFactory.apply(targetPeer);
        this.stopTime = stopTime;
        this.retryCount = retryCount;
    }

    Peer targetPeer() {
        return targetPeer;
    }

    NetworkMessage request() {
        return request;
    }

    long stopTime() {
        return stopTime;
    }

    int retryCount() {
        return retryCount;
    }

    Set<Peer> unavailablePeers() {
        return unavailablePeers;
    }

    RetryContext nextAttempt(Peer newTargetPeer) {
        // We can avoid recreating the request if the target peer has not changed.
        if (!newTargetPeer.equals(targetPeer)) {
            request = requestFactory.apply(newTargetPeer);
        }

        targetPeer = newTargetPeer;

        retryCount++;

        return this;
    }

    RetryContext nextAttemptForUnavailablePeer(Peer newTargetPeer) {
        unavailablePeers.add(targetPeer);

        return nextAttempt(newTargetPeer);
    }
}
