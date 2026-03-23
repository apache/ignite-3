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

import static org.apache.ignite.internal.network.NetworkMessageTypes.HANDSHAKE_FINISH;

import org.apache.ignite.internal.network.annotations.Transferable;

/**
 * Handshake finish message, contains the quantity of the received messages.
 * This message is sent from an acceptor to an initiator as a response to the {@link HandshakeStartResponseMessage}.
 */
@Transferable(HANDSHAKE_FINISH)
public interface HandshakeFinishMessage extends InternalMessage {
    /**
     * Returns number of received messages.
     *
     * @return Number of received messages.
     */
    long receivedCount();
}
