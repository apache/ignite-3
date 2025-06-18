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

import org.jetbrains.annotations.Nullable;

/**
 * Context for throttling requests to a peer from a client of a replication group.
 */
public interface ThrottlingContextHolder {
    /**
     * Whether the peer is overloaded. This is an assumption of the client side this can be based on
     * the current load on the peer.
     */
    boolean isOverloaded();

    /**
     * Called before sending a request.
     */
    void beforeRequest();

    /**
     * Called after sending a request.
     *
     * @param requestStartTimestamp Timestamp when the request was sent.
     * @param retriableError Whether the Raft error is retriable. {@code null} if there was no error.
     */
    void afterRequest(long requestStartTimestamp, @Nullable Boolean retriableError);

    /**
     * Returns the timeout for a request to the peer.
     *
     * @return Timeout in milliseconds.
     */
    long peerRequestTimeoutMillis();

    /**
     * Returns the context holder for a peer with the given consistent ID.
     *
     * @param consistentId Consistent ID of the peer.
     * @return ThrottlingContextHolder for the peer.
     */
    ThrottlingContextHolder peerContextHolder(String consistentId);

    /**
     * Called when a node leaves the cluster.
     *
     * @param consistentId Consistent ID of the node that left.
     */
    void onNodeLeft(String consistentId);
}
