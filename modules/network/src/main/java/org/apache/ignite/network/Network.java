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

package org.apache.ignite.network;

import java.util.HashMap;
import java.util.Map;

/**
 * Entry point for network module.
 */
public class Network {
    /** Message mappers map, message type -> message mapper. */
    private final Map<Short, MessageMapper> messageMappers = new HashMap<>();

    /** Message handlers. */
    private final MessageHandlerHolder messageHandlerHolder = new MessageHandlerHolder();

    /** Cluster factory. */
    private final NetworkClusterFactory clusterFactory;

    /**
     * Constructor.
     * @param factory Cluster factory.
     */
    public Network(NetworkClusterFactory factory) {
        clusterFactory = factory;
    }

    /**
     * Register message mapper by message type.
     * @param type Message type.
     * @param messageMapper Message mapper.
     */
    public void registerMessageMapper(short type, MessageMapper messageMapper) {
        this.messageMappers.put(type, messageMapper);
    }

    /**
     * Start new cluster.
     * @return Network cluster.
     */
    public NetworkCluster start() {
        NetworkClusterContext context = new NetworkClusterContext(messageHandlerHolder, messageMappers);
        return clusterFactory.startCluster(context);
    }
}
