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

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.net.Address;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.schemas.network.ClusterMembershipView;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.configuration.schemas.network.NetworkView;
import org.apache.ignite.configuration.schemas.network.ScaleCubeView;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.netty.ConnectionManager;
import org.apache.ignite.internal.network.recovery.RecoveryClientHandshakeManager;
import org.apache.ignite.internal.network.recovery.RecoveryServerHandshakeManager;
import org.apache.ignite.network.AbstractClusterService;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.NettyBootstrapFactory;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeFinder;
import org.apache.ignite.network.NodeFinderFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;

/**
 * {@link ClusterServiceFactory} implementation that uses ScaleCube for messaging and topology services.
 */
public class ScaleCubeClusterServiceFactory implements ClusterServiceFactory {
    /** {@inheritDoc} */
    @Override
    public ClusterService createClusterService(ClusterLocalConfiguration context, NetworkConfiguration networkConfiguration) {
        final String consistentId = context.getName();
    
        var nettyBootstrapFactory = new NettyBootstrapFactory(networkConfiguration.value(), consistentId);
        
        return createClusterService(context, networkConfiguration, nettyBootstrapFactory);
    }
    
    /**
     * Creates a new {@link ClusterService} using the provided context. The created network will not be in the "started" state.
     *
     * @param context               Cluster context.
     * @param networkConfiguration  Network configuration.
     * @param nettyBootstrapFactory Bootstrap factory.
     * @return New cluster service.
     */
    public ClusterService createClusterService(
            ClusterLocalConfiguration context,
            NetworkConfiguration networkConfiguration,
            NettyBootstrapFactory nettyBootstrapFactory
    ) {
        var topologyService = new ScaleCubeTopologyService();

        var messagingService = new ScaleCubeMessagingService();
    
        return new AbstractClusterService(context, topologyService, messagingService) {
            private volatile ClusterImpl cluster;

            private volatile ConnectionManager connectionMgr;

            /** {@inheritDoc} */
            @Override
            public void start() {
                String consistentId = context.getName();

                MessageSerializationRegistry registry = context.getSerializationRegistry();

                UUID launchId = UUID.randomUUID();

                var messageFactory = new NetworkMessagesFactory();

                NetworkView configView = networkConfiguration.value();

                connectionMgr = new ConnectionManager(
                        configView,
                        registry,
                        consistentId,
                        () -> new RecoveryServerHandshakeManager(launchId, consistentId, messageFactory),
                        () -> new RecoveryClientHandshakeManager(launchId, consistentId, messageFactory),
                        nettyBootstrapFactory
                );

                var transport = new ScaleCubeDirectMarshallerTransport(connectionMgr, topologyService, messageFactory);

                NodeFinder finder = NodeFinderFactory.createNodeFinder(configView.nodeFinder());

                cluster = new ClusterImpl(clusterConfig(configView.membership()))
                        .handler(cl -> new ClusterMessageHandler() {
                            /** {@inheritDoc} */
                            @Override
                            public void onMessage(Message message) {
                                messagingService.fireEvent(message);
                            }

                            /** {@inheritDoc} */
                            @Override
                            public void onMembershipEvent(MembershipEvent event) {
                                topologyService.onMembershipEvent(event);
                            }
                        })
                        .config(opts -> opts.memberAlias(consistentId))
                        .transport(opts -> opts.transportFactory(new DelegatingTransportFactory(messagingService, config -> transport)))
                        .membership(opts -> opts.seedMembers(parseAddresses(finder.findNodes())));

                // resolve cyclic dependencies
                messagingService.setCluster(cluster);
                topologyService.setCluster(cluster);

                connectionMgr.start();

                cluster.startAwait();

                // emit an artificial event as if the local member has joined the topology (ScaleCube doesn't do that)
                var localMembershipEvent = MembershipEvent.createAdded(cluster.member(), null, System.currentTimeMillis());

                topologyService.onMembershipEvent(localMembershipEvent);
            }

            /** {@inheritDoc} */
            @Override
            public void stop() {
                // local member will be null, if cluster has not been started
                if (cluster.member() == null) {
                    return;
                }

                cluster.shutdown();
                cluster.onShutdown().block();

                connectionMgr.stop();
            }

            /** {@inheritDoc} */
            @Override
            public void beforeNodeStop() {
                stop();
            }

            /** {@inheritDoc} */
            @Override
            public boolean isStopped() {
                return cluster.isShutdown();
            }
        };
    }

    /**
     * Returns ScaleCube's cluster configuration. Can be overridden in subclasses for finer control of the created {@link ClusterService}
     * instances.
     *
     * @param cfg Membership configuration.
     * @return Cluster configuration.
     */
    protected ClusterConfig clusterConfig(ClusterMembershipView cfg) {
        ScaleCubeView scaleCube = cfg.scaleCube();

        return ClusterConfig.defaultLocalConfig()
                .membership(opts ->
                        opts.syncInterval(cfg.membershipSyncInterval())
                                .suspicionMult(scaleCube.membershipSuspicionMultiplier())
                )
                .failureDetector(opts ->
                        opts.pingInterval(cfg.failurePingInterval())
                                .pingReqMembers(scaleCube.failurePingRequestMembers())
                )
                .gossip(opts -> opts.gossipInterval(scaleCube.gossipInterval()));
    }

    /**
     * Converts the given list of {@link NetworkAddress} into a list of ScaleCube's {@link Address}.
     *
     * @param addresses Network address.
     * @return List of ScaleCube's {@link Address}.
     */
    private static List<Address> parseAddresses(List<NetworkAddress> addresses) {
        return addresses.stream()
                .map(addr -> Address.create(addr.host(), addr.port()))
                .collect(Collectors.toList());
    }
}
