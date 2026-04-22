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

package org.apache.ignite.internal.client;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;

/**
 * Thin client payload output channel.
 */
public class PayloadOutputChannel implements AutoCloseable {
    /** Client channel. */
    private final ClientChannel ch;

    /** Output stream. */
    private final ClientMessagePacker out;

    /** Client request ID. */
    private final long requestId;

    /** Actions to be executed when the payload is sent. */
    private final List<Runnable> onSentActions;

    /**
     * Constructor.
     *
     * @param ch  Channel.
     * @param out Packer.
     * @param requestId Request ID.
     */
    PayloadOutputChannel(ClientChannel ch, ClientMessagePacker out, long requestId) {
        this.ch = ch;
        this.out = out;
        this.requestId = requestId;
        this.onSentActions = new ArrayList<>();
    }

    /**
     * Gets client channel.
     *
     * @return Client channel.
     */
    public ClientChannel clientChannel() {
        return ch;
    }

    /**
     * Gets the unpacker.
     *
     * @return Unpacker.
     */
    public ClientMessagePacker out() {
        return out;
    }

    /**
     * Gets client request ID.
     *
     * @return Client request ID.
     */
    public long requestId() {
        return requestId;
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        out.close();
    }

    /**
     * Sets the action to be executed when the payload is sent successfully.
     *
     * @param action Action to be executed.
     */
    public void onSent(Runnable action) {
        this.onSentActions.add(action);
    }

    /** Returns an action, if any, that should be executed when the payload is sent successfully. */
    List<Runnable> onSentActions() {
        return onSentActions;
    }
}
