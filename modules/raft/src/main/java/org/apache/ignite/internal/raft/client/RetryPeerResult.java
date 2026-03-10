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
import org.jetbrains.annotations.Nullable;

/**
 * Result of attempting to select a peer for retry.
 */
final class RetryPeerResult {
    private final @Nullable Peer peer;
    private final @Nullable Throwable error;

    private RetryPeerResult(@Nullable Peer peer, @Nullable Throwable error) {
        this.peer = peer;
        this.error = error;
    }

    static RetryPeerResult success(Peer peer) {
        return new RetryPeerResult(peer, null);
    }

    static RetryPeerResult fail(Throwable error) {
        return new RetryPeerResult(null, error);
    }

    boolean isSuccess() {
        return peer != null;
    }

    Peer peer() {
        assert peer != null : "Check isSuccess() before calling peer()";
        return peer;
    }

    Throwable error() {
        assert error != null : "Check isSuccess() before calling error()";
        return error;
    }
}
