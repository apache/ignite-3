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
package org.apache.ignite.network.message;

import org.apache.ignite.network.NetworkMember;

/**
 * Message for exchange information in cluster.
 */
public abstract class NetworkMessage {
    /** Network member who sent this message. */
    private NetworkMember sender;

    /**
     * Constructor.
     */
    public NetworkMessage() {
    }

    /**
     * Set sender.
     * @param sender Sender of this message.
     */
    public void sender(NetworkMember sender) {
        this.sender = sender;
    }

    /**
     * @return Network member who sent this message.
     */
    public NetworkMember sender() {
        return sender;
    }

    /**
     * @return Message type.
     */
    public abstract short type();
}
