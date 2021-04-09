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

import java.util.List;
import org.apache.ignite.network.message.MessageSerializerProviders;

/**
 * Network configuration of a node.
 *
 * TODO: migrate to common configuration class when it's available, see
 *  https://issues.apache.org/jira/browse/IGNITE-14496
 */
public class ClusterLocalConfiguration {
    /** Network alias of a node. */
    private final String name;

    /** Port. */
    private final int port;

    /** Addresses of other nodes. */
    private final List<String> memberAddresses;

    /** Message mapper providers. */
    private final MessageSerializerProviders messageSerializerProviders;

    /**
     * @param name Name.
     * @param port Port.
     * @param memberAddresses Member addresses.
     * @param messageSerializerProviders Message mapper providers.
     */
    public ClusterLocalConfiguration(
        String name, int port, List<String> memberAddresses, MessageSerializerProviders messageSerializerProviders
    ) {
        this.name = name;
        this.port = port;
        this.memberAddresses = List.copyOf(memberAddresses);
        this.messageSerializerProviders = messageSerializerProviders;
    }

    /**
     * Network alias of a node.
     */
    public String getName() {
        return name;
    }

    /**
     * Port.
     */
    public int getPort() {
        return port;
    }

    /**
     * Addresses of other nodes.
     */
    public List<String> getMemberAddresses() {
        return memberAddresses;
    }

    /**
     * Message mapper providers.
     */
    public MessageSerializerProviders getMessageMapperProviders() {
        return messageSerializerProviders;
    }
}
