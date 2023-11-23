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

import java.util.UUID;

/**
 * The HandshakeTieBreaker class provides a mechanism for determining whether an existing channel should be closed in case of a clinch
 * during a handshake process.
 *
 * <p>A clinch is a situation when two parallel handshakes (one from node A to B, another from B to A) acquire locks (now these are
 * recovery descriptors) on different sides, then each of them tries to take a lock on the opposite side, which is impossible as
 * it's already held by the corresponding competitor. To resolve such a deadlock, one of the handshakes must be terminated.
 */
class HandshakeTieBreaker {
    /**
     * Determines whether an existing channel should be closed based on the comparison of the server's launch id and the client's launch id.
     * If the client's launch id is greater than the server's launch id, the existing channel should be closed in favor of the new one;
     * otherwise, the new channel should be closed.
     *
     * @param serverLaunchId Server's launch id.
     * @param clientLaunchId Client's launch id.
     * @return {@code true} if an existing channel should be closed, {@code false} otherwise.
     */
    static boolean shouldCloseChannel(UUID serverLaunchId, UUID clientLaunchId) {
        return clientLaunchId.compareTo(serverLaunchId) > 0;
    }
}
