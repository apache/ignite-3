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

package org.apache.ignite.internal.network.recovery;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.network.OutNetworkObject;

/**
 * Recovery protocol descriptor.
 */
public class RecoveryDescriptor {
    /** Unacknowledged messages. */
    private final Queue<OutNetworkObject> unacknowledgedMessages;

    /** Count of sent messages. */
    private long sentCount;

    /** Count of acknowledged sent messages. */
    private long acknowledgedCount;

    /** Count of received messages. */
    private long receivedCount;

    /**
     * Constructor.
     *
     * @param queueLimit Maximum size of unacknowledged messages queue.
     */
    public RecoveryDescriptor(int queueLimit) {
        this.unacknowledgedMessages = new ArrayDeque<>(queueLimit);
    }

    /**
     * Returns count of received messages.
     *
     * @return Count of received messages.
     */
    public long receivedCount() {
        return receivedCount;
    }

    /**
     * Acknowledges that sent messages were received by the remote node.
     *
     * @param messagesReceivedByRemote Number of all messages received by the remote node.
     */
    public void acknowledge(long messagesReceivedByRemote) {
        while (acknowledgedCount < messagesReceivedByRemote) {
            OutNetworkObject req = unacknowledgedMessages.poll();

            assert req != null;

            acknowledgedCount++;
        }
    }

    /**
     * Returns the number of the messages unacknowledged by the remote node.
     *
     * @return The number of the messages unacknowledged by the remote node.
     */
    public int unacknowledgedCount() {
        long res = sentCount - acknowledgedCount;
        int size = unacknowledgedMessages.size();

        assert res >= 0;
        assert res == size;

        return size;
    }

    /**
     * Returns unacknowledged messages.
     *
     * @return Unacknowledged messages.
     */
    public List<OutNetworkObject> unacknowledgedMessages() {
        return new ArrayList<>(unacknowledgedMessages);
    }

    /**
     * Adds a sent message.
     *
     * @param msg Message.
     */
    public void add(OutNetworkObject msg) {
        msg.shouldBeSavedForRecovery(false);
        sentCount++;
        unacknowledgedMessages.add(msg);
    }

    /**
     * Handles the event of receiving a new message.
     *
     * @return Number of received messages.
     */
    public long onReceive() {
        receivedCount++;

        return receivedCount;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(RecoveryDescriptor.class, this);
    }
}
