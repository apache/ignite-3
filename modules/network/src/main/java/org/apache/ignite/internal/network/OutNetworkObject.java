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

package org.apache.ignite.internal.network;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.network.message.ClassDescriptorMessage;

/**
 * Wrapper for the outgoing network message.
 */
public class OutNetworkObject {
    /** Message. */
    private final NetworkMessage networkMessage;

    /** List of class descriptor messages. */
    private final List<ClassDescriptorMessage> descriptors;

    /**
     * Flag indicating if this outgoing message should be added to the unacknowledged messages queue of the recovery descriptor.
     * Acknowledgement message and handshake messages should not have this flag set to {@code true}. After adding the message
     * to the queue this flag should be set to {@code false}.
     */
    private boolean shouldBeSavedForRecovery;

    private final CompletableFuture<Void> acknowledgedFuture = new CompletableFuture<>();

    /**
     * Constructor.
     *
     * @param networkMessage Network message.
     * @param descriptors Class descriptors.
     */
    public OutNetworkObject(NetworkMessage networkMessage, List<ClassDescriptorMessage> descriptors) {
        this.networkMessage = networkMessage;
        this.descriptors = descriptors;
        this.shouldBeSavedForRecovery = networkMessage.needAck();
    }

    /**
     * Returns {@code true} if this should be saved for recovery. As soon as it is added to the unacknowledged messages queue,
     * this is changed to {@code false}.
     */
    public boolean shouldBeSavedForRecovery() {
        return shouldBeSavedForRecovery;
    }

    public void shouldBeSavedForRecovery(boolean shouldBeSavedForRecovery) {
        this.shouldBeSavedForRecovery = shouldBeSavedForRecovery;
    }

    public NetworkMessage networkMessage() {
        return networkMessage;
    }

    public List<ClassDescriptorMessage> descriptors() {
        return descriptors;
    }

    public CompletableFuture<Void> acknowledgedFuture() {
        return acknowledgedFuture;
    }

    public void acknowledge() {
        acknowledgedFuture.complete(null);
    }

    public void failAcknowledgement(Throwable ex) {
        acknowledgedFuture.completeExceptionally(ex);
    }
}
