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

package org.apache.ignite.network.scalecube;

import java.util.List;
import java.util.stream.Collectors;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.net.Address;
import org.apache.ignite.network.Network;
import org.apache.ignite.network.NetworkConfigurationException;
import org.apache.ignite.network.NetworkContext;
import org.apache.ignite.network.NetworkFactory;
import org.apache.ignite.network.NetworkImpl;

/**
 * {@link NetworkFactory} implementation that uses ScaleCube for messaging and topology services.
 */
public class ScaleCubeNetworkFactory implements NetworkFactory {
    /** {@inheritDoc} */
    @Override public Network createNetwork(NetworkContext context) {
        var memberResolver = new ScaleCubeMemberResolver();
        var topologyService = new ScaleCubeTopologyService(memberResolver);
        var messagingService = new ScaleCubeMessagingService(memberResolver);

        var cluster = new ClusterImpl()
            .handler(cl -> new ClusterMessageHandler() {
                @Override public void onMessage(Message message) {
                    messagingService.fireEvent(message);
                }

                @Override public void onMembershipEvent(MembershipEvent event) {
                    topologyService.fireEvent(event);
                }
            })
            .config(opts -> opts.memberAlias(context.getName()))
            .transport(opts -> opts
                .port(context.getPort())
                .messageCodec(new ScaleCubeMessageCodec(context.getMessageMapperProviders())))
            .membership(opts -> opts.seedMembers(parseAddresses(context.getMemberAddresses())));

        // resolve cyclic dependencies
        topologyService.setCluster(cluster);
        messagingService.setCluster(cluster);

        return new NetworkImpl(context, topologyService, messagingService) {
            @Override public void start() {
                cluster.startAwait();
            }

            @Override public void shutdown() {
                cluster.shutdown();
                cluster.onShutdown().block();
            }
        };
    }

    /** */
    private List<Address> parseAddresses(List<String> addresses) {
        try {
            return addresses.stream()
                .map(Address::from)
                .collect(Collectors.toList());
        } catch (IllegalArgumentException e) {
            throw new NetworkConfigurationException("Failed to parse address", e);
        }
    }
}
