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

package org.apache.ignite.internal.network.scalecube;

import static io.scalecube.cluster.membership.MembershipEvent.createAdded;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Network.ADDRESS_UNRESOLVED_ERR;

import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.metadata.MetadataCodec;
import io.scalecube.net.Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ChannelTypeRegistry;
import org.apache.ignite.internal.network.ClusterIdSupplier;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.DefaultMessagingService;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NettyBootstrapFactory;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.NodeFinder;
import org.apache.ignite.internal.network.NodeFinderFactory;
import org.apache.ignite.internal.network.TopologyEventHandler;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.network.configuration.ClusterMembershipView;
import org.apache.ignite.internal.network.configuration.NetworkConfiguration;
import org.apache.ignite.internal.network.configuration.NetworkView;
import org.apache.ignite.internal.network.configuration.ScaleCubeView;
import org.apache.ignite.internal.network.netty.ConnectionManager;
import org.apache.ignite.internal.network.recovery.StaleIds;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.network.serialization.SerializationService;
import org.apache.ignite.internal.network.serialization.UserObjectSerializationContext;
import org.apache.ignite.internal.network.serialization.marshal.DefaultUserObjectMarshaller;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.version.IgniteProductVersionSource;
import org.apache.ignite.internal.worker.CriticalWorkerRegistry;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NodeMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * ScaleCube-based implementation of {@link ClusterService}.
 */
public class ScaleCubeClusterService implements ClusterService {
    private static final IgniteLogger LOG = Loggers.forClass(ScaleCubeClusterService.class);

    private static final MetadataCodec METADATA_CODEC = MetadataCodec.INSTANCE;

    private final ClusterNodeImpl localNode;

    private final ScaleCubeTopologyService topologyService = new ScaleCubeTopologyService();

    private final NetworkMessagesFactory messageFactory = new NetworkMessagesFactory();

    private final DefaultMessagingService messagingService;

    private final MessageSerializationRegistry serializationRegistry;

    private final ConnectionManager connectionMgr;

    private final NodeFinder nodeFinder;

    private final NetworkConfiguration config;

    private final AtomicBoolean isStopped = new AtomicBoolean();

    @Nullable
    private volatile ClusterImpl cluster;

    /** Constructor. */
    public ScaleCubeClusterService(
            String consistentId,
            NetworkConfiguration networkConfiguration,
            NettyBootstrapFactory nettyBootstrapFactory,
            MessageSerializationRegistry serializationRegistry,
            StaleIds staleIds,
            ClusterIdSupplier clusterIdSupplier,
            CriticalWorkerRegistry criticalWorkerRegistry,
            FailureProcessor failureProcessor,
            ChannelTypeRegistry channelTypeRegistry,
            IgniteProductVersionSource productVersionSource,
            MetricManager metricManager
    ) {
        this.config = networkConfiguration;
        this.serializationRegistry = serializationRegistry;

        // Adding this handler as the first handler to make sure that StaleIds is at least up-to-date as any
        // other component that watches topology events.
        topologyService.addEventHandler(new TopologyEventHandler() {
            @Override
            public void onDisappeared(InternalClusterNode member) {
                staleIds.markAsStale(member.id());
            }
        });

        UserObjectSerializationContext userObjectSerialization = createUserObjectSerializationContext();

        NetworkView configView = networkConfiguration.value();

        InetSocketAddress localBindAddress = localBindAddress(configView);

        localNode = createClusterNode(consistentId, localBindAddress);

        var serializationService = new SerializationService(serializationRegistry, userObjectSerialization);

        connectionMgr = new ConnectionManager(
                configView,
                serializationService,
                localBindAddress,
                localNode,
                nettyBootstrapFactory,
                staleIds,
                clusterIdSupplier,
                channelTypeRegistry,
                productVersionSource,
                topologyService,
                failureProcessor
        );

        messagingService = new DefaultMessagingService(
                consistentId,
                messageFactory,
                topologyService,
                staleIds,
                userObjectSerialization.descriptorRegistry(),
                userObjectSerialization.marshaller(),
                criticalWorkerRegistry,
                failureProcessor,
                connectionMgr,
                metricManager,
                channelTypeRegistry
        );

        nodeFinder = NodeFinderFactory.createNodeFinder(configView.nodeFinder(), consistentId, localBindAddress);

        topologyService.addEventHandler(new TopologyEventHandler() {
            @Override
            public void onDisappeared(InternalClusterNode member) {
                connectionMgr.handleNodeLeft(member.id())
                        .thenRun(() -> nettyBootstrapFactory.handshakeEventLoopSwitcher().nodeLeftTopology(member));
            }
        });
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        // Cluster creation performs initial node names resolution which can take some time, therefore it must be created in the "start"
        // method.
        ClusterImpl cluster = createCluster();

        this.cluster = cluster;

        // Resolve cyclic dependencies.
        topologyService.setCluster(cluster);

        connectionMgr.start();
        messagingService.start();
        nodeFinder.start();
        cluster.startAwait();

        // Emit an artificial event as if the local member has joined the topology (ScaleCube doesn't do that).
        MembershipEvent localMembershipEvent = createAdded(cluster.member(), null, System.currentTimeMillis());
        topologyService.onMembershipEvent(localMembershipEvent);

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        assert isStopped.get() : "ClusterService must have been stopped in the \"beforeNodeStop\" method";

        return nullCompletedFuture();
    }

    @Override
    public void beforeNodeStop() {
        if (!isStopped.compareAndSet(false, true)) {
            return;
        }

        try {
            IgniteUtils.closeAll(
                    connectionMgr::initiateStopping,
                    this::shutdownCluster,
                    nodeFinder::close,
                    messagingService::stop,
                    connectionMgr::stop
            );
        } catch (Exception e) {
            throw new IgniteInternalException(INTERNAL_ERR, e);
        }
    }

    @Override
    public void updateMetadata(NodeMetadata metadata) {
        ClusterImpl cluster = this.cluster;

        assert cluster != null : "Cluster has not been started";

        cluster.updateMetadata(metadata).subscribe();

        topologyService.updateLocalMetadata(metadata);
    }

    @Override
    public String nodeName() {
        return localNode.name();
    }

    @Override
    public TopologyService topologyService() {
        return topologyService;
    }

    @Override
    public MessagingService messagingService() {
        return messagingService;
    }

    @Override
    public MessageSerializationRegistry serializationRegistry() {
        return serializationRegistry;
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
                        opts.syncInterval(cfg.membershipSyncIntervalMillis())
                                .suspicionMult(scaleCube.membershipSuspicionMultiplier())
                )
                .failureDetector(opts ->
                        opts.pingInterval(cfg.failurePingIntervalMillis())
                                .pingReqMembers(scaleCube.failurePingRequestMembers())
                )
                .gossip(opts ->
                        opts.gossipInterval(scaleCube.gossipIntervalMillis())
                                .gossipRepeatMult(scaleCube.gossipRepeatMult())
                )
                .metadataTimeout(scaleCube.metadataTimeoutMillis());
    }

    private ClusterImpl createCluster() {
        var transport = new ScaleCubeDirectMarshallerTransport(parseAddress(localNode.address()), messagingService, messageFactory);

        ClusterConfig clusterConfig = clusterConfig(config.membership().value())
                .memberId(localNode.id().toString())
                .memberAlias(localNode.name())
                .transport(opts -> opts.transportFactory(transportConfig -> transport))
                .membership(opts -> opts.seedMembers(parseAddresses(nodeFinder.findNodes())))
                .metadataCodec(METADATA_CODEC);

        return new ClusterImpl(clusterConfig)
                .handler(cl -> new ClusterMessageHandler() {
                    @Override
                    public void onMembershipEvent(MembershipEvent event) {
                        topologyService.onMembershipEvent(event);
                    }
                });
    }

    private static InetSocketAddress localBindAddress(NetworkView configView) {
        int port = configView.port();

        String[] addresses = configView.listenAddresses();

        if (addresses.length == 0) {
            return new InetSocketAddress(port);
        } else {
            if (addresses.length > 1) {
                // TODO: IGNITE-22369 - support more than one listen address.
                throw new IgniteException(
                        INTERNAL_ERR, "Only one listen address is allowed for now, but got " + Arrays.toString(addresses)
                );
            }

            return new InetSocketAddress(addresses[0], port);
        }
    }

    private static ClusterNodeImpl createClusterNode(String consistentId, InetSocketAddress localAddress) {
        InetAddress address = localAddress.getAddress();

        try {
            String host = address.isAnyLocalAddress() ? InetAddress.getLocalHost().getHostAddress() : address.getHostAddress();

            var networkAddress = new NetworkAddress(host, localAddress.getPort());

            return new ClusterNodeImpl(UUID.randomUUID(), consistentId, networkAddress);
        } catch (UnknownHostException e) {
            throw new IgniteInternalException(ADDRESS_UNRESOLVED_ERR, "Failed to resolve local host address.", e);
        }
    }

    private static UserObjectSerializationContext createUserObjectSerializationContext() {
        var userObjectDescriptorRegistry = new ClassDescriptorRegistry();
        var userObjectDescriptorFactory = new ClassDescriptorFactory(userObjectDescriptorRegistry);

        var userObjectMarshaller = new DefaultUserObjectMarshaller(userObjectDescriptorRegistry, userObjectDescriptorFactory);

        return new UserObjectSerializationContext(userObjectDescriptorRegistry, userObjectDescriptorFactory, userObjectMarshaller);
    }

    private static List<Address> parseAddresses(Collection<NetworkAddress> addresses) {
        return addresses.stream()
                .map(ScaleCubeClusterService::parseAddress)
                .collect(Collectors.toList());
    }

    private static Address parseAddress(NetworkAddress address) {
        return Address.create(address.host(), address.port());
    }

    private void shutdownCluster() {
        ClusterImpl cluster = this.cluster;

        // Local member will be null if the cluster has not been started.
        if (cluster == null || cluster.member() == null) {
            return;
        }

        cluster.shutdown();

        try {
            cluster.onShutdown().toFuture().get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInternalException(INTERNAL_ERR, "Interrupted while waiting for the ClusterService to stop", e);
        } catch (ExecutionException e) {
            throw new IgniteInternalException(INTERNAL_ERR, "Unable to stop the ClusterService", e.getCause());
        } catch (TimeoutException e) {
            // Failed to leave gracefully.
            LOG.warn("Failed to wait for ScaleCube cluster shutdown [reason={}]", e, e.getMessage());
        }
    }
}
