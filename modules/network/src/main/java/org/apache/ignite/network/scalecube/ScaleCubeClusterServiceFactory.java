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

package org.apache.ignite.network.scalecube;

import static io.scalecube.cluster.membership.MembershipEvent.createAdded;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.metadata.MetadataCodec;
import io.scalecube.net.Address;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.configuration.ClusterMembershipView;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.configuration.NetworkView;
import org.apache.ignite.internal.network.configuration.ScaleCubeView;
import org.apache.ignite.internal.network.netty.ConnectionManager;
import org.apache.ignite.internal.network.recovery.StaleIds;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.SerializationService;
import org.apache.ignite.internal.network.serialization.UserObjectSerializationContext;
import org.apache.ignite.internal.network.serialization.marshal.DefaultUserObjectMarshaller;
import org.apache.ignite.network.AbstractClusterService;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.DefaultMessagingService;
import org.apache.ignite.network.NettyBootstrapFactory;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeFinder;
import org.apache.ignite.network.NodeFinderFactory;
import org.apache.ignite.network.NodeMetadata;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;

/**
 * Cluster service factory that uses ScaleCube for messaging and topology services.
 */
public class ScaleCubeClusterServiceFactory {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ScaleCubeClusterServiceFactory.class);

    /** Metadata codec. */
    private static final MetadataCodec METADATA_CODEC = MetadataCodec.INSTANCE;

    /**
     * Creates a new {@link ClusterService} using the provided context. The created network will not be in the "started" state.
     *
     * @param networkConfiguration  Network configuration.
     * @param nettyBootstrapFactory Bootstrap factory.
     * @return New cluster service.
     */
    public ClusterService createClusterService(
            String consistentId,
            NetworkConfiguration networkConfiguration,
            NettyBootstrapFactory nettyBootstrapFactory,
            MessageSerializationRegistry serializationRegistry,
            StaleIds staleIds
    ) {
        var topologyService = new ScaleCubeTopologyService();

        // Adding this handler as the first handler to make sure that StaleIds is at least up-to-date as any
        // other component that watches topology events.
        topologyService.addEventHandler(new TopologyEventHandler() {
            @Override
            public void onDisappeared(ClusterNode member) {
                staleIds.markAsStale(member.id());
            }
        });

        var messageFactory = new NetworkMessagesFactory();

        UserObjectSerializationContext userObjectSerialization = createUserObjectSerializationContext();

        var messagingService = new DefaultMessagingService(
                consistentId,
                messageFactory,
                topologyService,
                staleIds,
                userObjectSerialization.descriptorRegistry(),
                userObjectSerialization.marshaller()
        );

        return new AbstractClusterService(consistentId, topologyService, messagingService, serializationRegistry) {

            private volatile ClusterImpl cluster;

            private volatile ConnectionManager connectionMgr;

            private volatile CompletableFuture<Void> shutdownFuture;

            @Override
            public CompletableFuture<Void> start() {
                var serializationService = new SerializationService(serializationRegistry, userObjectSerialization);

                UUID launchId = UUID.randomUUID();

                NetworkView configView = networkConfiguration.value();

                ConnectionManager connectionMgr = new ConnectionManager(
                        configView,
                        serializationService,
                        launchId,
                        consistentId,
                        nettyBootstrapFactory,
                        staleIds
                );
                this.connectionMgr = connectionMgr;

                connectionMgr.start();

                topologyService.addEventHandler(new TopologyEventHandler() {
                    @Override
                    public void onDisappeared(ClusterNode member) {
                        connectionMgr.handleNodeLeft(member.id());
                    }
                });

                var transport = new ScaleCubeDirectMarshallerTransport(
                        connectionMgr.localAddress(),
                        messagingService,
                        topologyService,
                        messageFactory
                );

                NodeFinder finder = NodeFinderFactory.createNodeFinder(configView.nodeFinder());
                ClusterImpl cluster = new ClusterImpl(clusterConfig(configView.membership()))
                        .handler(cl -> new ClusterMessageHandler() {
                            @Override
                            public void onMembershipEvent(MembershipEvent event) {
                                topologyService.onMembershipEvent(event);
                            }
                        })
                        .config(opts -> opts
                                .memberId(launchId.toString())
                                .memberAlias(consistentId)
                                .metadataCodec(METADATA_CODEC)
                        )
                        .transport(opts -> opts.transportFactory(transportConfig -> transport))
                        .membership(opts -> opts.seedMembers(parseAddresses(finder.findNodes())));

                this.shutdownFuture = cluster.onShutdown().toFuture();

                // resolve cyclic dependencies
                topologyService.setCluster(cluster);
                messagingService.setConnectionManager(connectionMgr);

                cluster.startAwait();

                // emit an artificial event as if the local member has joined the topology (ScaleCube doesn't do that)
                var localMembershipEvent = createAdded(cluster.member(), null, System.currentTimeMillis());
                topologyService.onMembershipEvent(localMembershipEvent);

                this.cluster = cluster;

                return nullCompletedFuture();
            }

            @Override
            public void stop() {
                ConnectionManager localConnectionMgr = connectionMgr;

                if (localConnectionMgr != null) {
                    localConnectionMgr.initiateStopping();
                }

                // Local member will be null, if cluster has not been started.
                if (cluster != null && cluster.member() != null) {
                    cluster.shutdown();

                    try {
                        shutdownFuture.get(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();

                        throw new IgniteInternalException("Interrupted while waiting for the ClusterService to stop", e);
                    } catch (ExecutionException e) {
                        throw new IgniteInternalException("Unable to stop the ClusterService", e.getCause());
                    } catch (TimeoutException e) {
                        // Failed to leave gracefully.
                        LOG.warn("Failed to wait for ScaleCube cluster shutdown [reason={}]", e, e.getMessage());
                    }

                }

                if (localConnectionMgr != null) {
                    localConnectionMgr.stop();
                }

                // Messaging service checks connection manager's status before sending a message, so connection manager should be
                // stopped before messaging service
                messagingService.stop();
            }

            @Override
            public void beforeNodeStop() {
                stop();
            }

            @Override
            public boolean isStopped() {
                return shutdownFuture.isDone();
            }

            @Override
            public void updateMetadata(NodeMetadata metadata) {
                cluster.updateMetadata(metadata).subscribe();
                topologyService.updateLocalMetadata(metadata);
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
                .gossip(opts ->
                        opts.gossipInterval(scaleCube.gossipInterval())
                                .gossipRepeatMult(scaleCube.gossipRepeatMult())
                )
                .metadataTimeout(scaleCube.metadataTimeout());
    }

    /**
     * Creates everything that is needed for the user object serialization.
     *
     * @return User object serialization context.
     */
    private UserObjectSerializationContext createUserObjectSerializationContext() {
        var userObjectDescriptorRegistry = new ClassDescriptorRegistry();
        var userObjectDescriptorFactory = new ClassDescriptorFactory(userObjectDescriptorRegistry);

        var userObjectMarshaller = new DefaultUserObjectMarshaller(userObjectDescriptorRegistry, userObjectDescriptorFactory);

        return new UserObjectSerializationContext(userObjectDescriptorRegistry, userObjectDescriptorFactory,
                userObjectMarshaller);
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
