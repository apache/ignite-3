/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.network.internal.recovery.message;

import java.util.UUID;
import org.apache.ignite.network.message.NetworkMessage;

/**
 * Handshake start response message.
 */
public class HandshakeStartResponseMessage implements NetworkMessage {
    /** */
    public static final byte TYPE = 3;

    /** Launch id. */
    private final UUID launchId;

    /** Consistent id. */
    private final String consistentId;

    /** Number of received messages. */
    private final long rcvCnt;

    /** Connections count. */
    private final long connectCnt;

    public HandshakeStartResponseMessage(UUID launchId, String consistentId, long rcvCnt, long connectCnt) {
        this.launchId = launchId;
        this.consistentId = consistentId;
        this.rcvCnt = rcvCnt;
        this.connectCnt = connectCnt;
    }

    /**
     * @return Launch id.
     */
    public UUID launchId() {
        return launchId;
    }

    /**
     * @return Consistent id.
     */
    public String consistentId() {
        return consistentId;
    }

    /**
     * @return Number of received messages.
     */
    public long receivedCount() {
        return rcvCnt;
    }

    /**
     * @return Connections count.
     */
    public long connectCount() {
        return connectCnt;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE;
    }
}
