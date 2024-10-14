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
 */
class RetryContext {
    private final Peer targetPeer;

    private final Function<Peer, ? extends NetworkMessage> requestFactory;

    private final NetworkMessage request;

    private final long stopTime;

    private final int retryCount;

    private final Set<Peer> unavailablePeers;

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
        this(targetPeer, requestFactory, requestFactory.apply(targetPeer), stopTime, retryCount, Set.of());
    }

    private RetryContext(
            Peer targetPeer,
            Function<Peer, ? extends NetworkMessage> requestFactory,
            NetworkMessage request,
            long stopTime,
            int retryCount,
            Set<Peer> unavailablePeers
    ) {
        this.targetPeer = targetPeer;
        this.requestFactory = requestFactory;
        this.request = request;
        this.stopTime = stopTime;
        this.retryCount = retryCount;
        this.unavailablePeers = unavailablePeers;
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
        NetworkMessage newRequest = newTargetPeer.equals(targetPeer) ? request : requestFactory.apply(newTargetPeer);

        return new RetryContext(
                newTargetPeer,
                requestFactory,
                newRequest,
                stopTime,
                retryCount + 1,
                unavailablePeers
        );
    }

    RetryContext nextAttemptForUnavailablePeer(Peer newTargetPeer) {
        // We can avoid recreating the request if the target peer has not changed.
        NetworkMessage newRequest = newTargetPeer.equals(targetPeer) ? request : requestFactory.apply(newTargetPeer);

        Set<Peer> newUnavailablePeers = new HashSet<>(unavailablePeers);
        newUnavailablePeers.add(targetPeer);

        return new RetryContext(
                newTargetPeer,
                requestFactory,
                newRequest,
                stopTime,
                retryCount + 1,
                newUnavailablePeers
        );
    }
}
