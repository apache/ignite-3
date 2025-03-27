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

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toUnmodifiableList;

import java.net.InetSocketAddress;
import java.util.Arrays;
import org.apache.ignite.internal.network.configuration.MulticastNodeFinderConfigurationSchema;
import org.apache.ignite.internal.network.configuration.MulticastNodeFinderView;
import org.apache.ignite.internal.network.configuration.NodeFinderView;
import org.apache.ignite.internal.network.configuration.StaticNodeFinderConfigurationSchema;
import org.apache.ignite.internal.network.configuration.StaticNodeFinderView;
import org.apache.ignite.network.NetworkAddress;

/**
 * {@link NodeFinder} factory.
 */
public class NodeFinderFactory {
    /**
     * Creates a {@link NodeFinder} based on the provided configuration.
     *
     * @param nodeFinderConfiguration Node finder configuration.
     * @param nodeName Node name.
     * @return Node finder.
     */
    public static NodeFinder createNodeFinder(NodeFinderView nodeFinderConfiguration, String nodeName, InetSocketAddress localAddress) {
        switch (nodeFinderConfiguration.type()) {
            case StaticNodeFinderConfigurationSchema.TYPE:
                StaticNodeFinderView staticConfig = (StaticNodeFinderView) nodeFinderConfiguration;

                return Arrays.stream(staticConfig.netClusterNodes())
                        .map(NetworkAddress::from)
                        .collect(collectingAndThen(toUnmodifiableList(), StaticNodeFinder::new));
            case MulticastNodeFinderConfigurationSchema.TYPE:
                MulticastNodeFinderView multicastConfig = (MulticastNodeFinderView) nodeFinderConfiguration;

                return new MulticastNodeFinder(
                        multicastConfig.group(),
                        multicastConfig.port(),
                        multicastConfig.resultWaitTime(),
                        multicastConfig.ttl(),
                        nodeName,
                        localAddress
                );
            default:
                throw new IllegalArgumentException("Unsupported NodeFinder type " + nodeFinderConfiguration.type());
        }
    }
}
