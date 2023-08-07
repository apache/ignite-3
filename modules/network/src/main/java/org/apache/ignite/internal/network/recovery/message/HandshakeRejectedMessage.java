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

package org.apache.ignite.internal.network.recovery.message;

import static org.apache.ignite.internal.network.NetworkMessageTypes.HANDSHAKE_REJECTED;

import org.apache.ignite.network.annotations.Transferable;

/**
 * Handshake rejected message, contains the reason for a rejection.
 * This message is sent from a server to a client or wise versa.
 * After this message is received it makes no sense to retry connections with same node identity (launch ID must be changed
 * to make a retry).
 */
@Transferable(HANDSHAKE_REJECTED)
public interface HandshakeRejectedMessage extends InternalMessage {
    /**
     * Returns rejection reason.
     *
     * @return Reason of the rejection.
     */
    String reason();

    /**
     * Returns {@code true} iff the rejection is not expected and should be treated as a critical failure (requiring
     * the rejected node to restart).
     */
    boolean critical();
}
