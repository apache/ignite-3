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

package org.apache.ignite.internal.raft.client;

import org.apache.ignite.internal.raft.Peer;

/**
 * Strategy for executing retries. Each method receives everything needed to perform the retry.
 *
 * <p>This interface abstracts the differences between single-attempt mode and leader-wait mode:
 * <ul>
 *     <li><b>Single-attempt mode</b>: Each peer is tried at most once. All errors mark the peer
 *         as unavailable. When all peers exhausted, fail immediately.</li>
 *     <li><b>Leader-wait mode</b>: Transient errors retry on the same peer with delay.
 *         "No leader" errors track peers separately. When exhausted, wait for leader notification.</li>
 * </ul>
 */
interface RetryExecutionStrategy {
    /**
     * Executes retry on the specified peer with the given tracking for the current peer.
     *
     * @param context Current retry context.
     * @param nextPeer Peer to retry on.
     * @param trackCurrentAs How to track the current peer ({@link PeerTracking#COMMON} for "don't track").
     * @param reason Human-readable reason for the retry.
     */
    void executeRetry(RetryContext context, Peer nextPeer, PeerTracking trackCurrentAs, String reason);

    /**
     * Called when all peers have been exhausted.
     *
     * <p>In leader-wait mode, this triggers waiting for leader notification.
     * In single-attempt mode, this completes with {@link org.apache.ignite.internal.raft.ReplicationGroupUnavailableException}.
     */
    void onAllPeersExhausted();

    /**
     * Whether to track "no leader" peers separately from unavailable peers.
     *
     * <p>In leader-wait mode, peers that return "no leader" are tracked separately so that
     * when all peers are exhausted, the strategy can wait for a leader notification rather
     * than failing immediately. In single-attempt mode, all errors are treated uniformly
     * as unavailable.
     */
    boolean trackNoLeaderSeparately();

    /**
     * Returns the target peer strategy used for this request.
     */
    TargetPeerStrategy targetSelectionStrategy();
}
