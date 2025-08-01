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

package org.apache.ignite.internal;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.ClusterConfiguration.configOverrides;
import static org.apache.ignite.internal.ClusterConfiguration.containsOverrides;
import static org.apache.ignite.internal.ReplicationGroupsUtils.tablePartitionIds;
import static org.apache.ignite.internal.ReplicationGroupsUtils.zonePartitionIds;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.colocationEnabled;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedIn;
import static org.apache.ignite.internal.util.CollectionUtils.setListAtIndex;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.table.NodeUtils;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.TestInfo;

/**
 * Cluster of nodes used for testing.
 */
public class Cluster {
    private static final IgniteLogger LOG = Loggers.forClass(Cluster.class);

    private final ClusterConfiguration clusterConfiguration;

    /** Embedded nodes. */
    private final List<IgniteServer> igniteServers = new CopyOnWriteArrayList<>();

    /** Cluster nodes. */
    private final List<Ignite> nodes = new CopyOnWriteArrayList<>();

    private volatile boolean started = false;

    private volatile boolean stopped = false;

    /** Number of nodes in the cluster on first startAndInit() [if it was invoked]. */
    private volatile int initialClusterSize;

    /** Number of seeds to use instead of initialClusterSize. */
    private volatile int seedCountOverride = -1;

    /** Indices of nodes that have been knocked out. */
    private final Set<Integer> knockedOutNodesIndices = ConcurrentHashMap.newKeySet();

    private List<IgniteServer> metaStorageAndCmgNodes = List.of();

    /**
     * Creates a new cluster.
     */
    public Cluster(ClusterConfiguration clusterConfiguration) {
        this.clusterConfiguration = clusterConfiguration;
    }

    /**
     * Starts the cluster with the given number of nodes and initializes it.
     *
     * @param nodeCount Number of nodes in the cluster.
     */
    public void startAndInit(int nodeCount) {
        startAndInit(nodeCount, new int[] { 0 });
    }

    /**
     * Starts the cluster with the given number of nodes and initializes it.
     *
     * @param nodeCount Number of nodes in the cluster.
     * @param cmgNodes Indices of CMG nodes.
     */
    public void startAndInit(int nodeCount, int[] cmgNodes) {
        startAndInit(null, nodeCount, cmgNodes, builder -> {});
    }

    /**
     * Starts the cluster with the given number of nodes and initializes it.
     *
     * @param nodeCount Number of nodes in the cluster.
     * @param initParametersConfigurator Configure {@link InitParameters} before initializing the cluster.
     */
    public void startAndInit(int nodeCount, Consumer<InitParametersBuilder> initParametersConfigurator) {
        startAndInit(null, nodeCount, new int[]{0}, initParametersConfigurator);
    }

    /**
     * Starts the cluster with the given number of nodes and initializes it.
     *
     * @param testInfo Test info.
     * @param nodeCount Number of nodes in the cluster.
     * @param cmgNodes Indices of CMG nodes.
     * @param initParametersConfigurator Configure {@link InitParameters} before initializing the cluster.
     */
    public void startAndInit(
            @Nullable TestInfo testInfo,
            int nodeCount,
            int[] cmgNodes,
            Consumer<InitParametersBuilder> initParametersConfigurator
    ) {
        startAndInit(
                testInfo,
                nodeCount,
                cmgNodes,
                clusterConfiguration.defaultNodeBootstrapConfigTemplate(),
                initParametersConfigurator,
                NodeBootstrapConfigUpdater.noop()
        );
    }

    /**
     * Starts the cluster with the given number of nodes and initializes it with CMG on first node.
     *
     * @param nodeCount Number of nodes in the cluster.
     * @param nodeBootstrapConfigTemplate Node bootstrap config template to be used for each node started
     *     with this call.
     * @param initParametersConfigurator Configure {@link InitParameters} before initializing the cluster.
     */
    public void startAndInit(
            int nodeCount,
            String nodeBootstrapConfigTemplate,
            Consumer<InitParametersBuilder> initParametersConfigurator
    ) {
        startAndInit(
                null,
                nodeCount,
                new int[] { 0 },
                nodeBootstrapConfigTemplate,
                initParametersConfigurator,
                NodeBootstrapConfigUpdater.noop()
        );
    }

    /**
     * Starts the cluster with the given number of nodes and initializes it.
     *
     * @param testInfo Test info.
     * @param nodeCount Number of nodes in the cluster.
     * @param cmgNodes Indices of CMG nodes.
     * @param nodeBootstrapConfigTemplate Node bootstrap config template to be used for each node started
     *     with this call.
     * @param initParametersConfigurator Configure {@link InitParameters} before initializing the cluster.
     * @param nodeBootstrapConfigUpdater Boot configuration updater before starting the node.
     */
    private void startAndInit(
            @Nullable TestInfo testInfo,
            int nodeCount,
            int[] cmgNodes,
            String nodeBootstrapConfigTemplate,
            Consumer<InitParametersBuilder> initParametersConfigurator,
            NodeBootstrapConfigUpdater nodeBootstrapConfigUpdater
    ) {
        if (started) {
            throw new IllegalStateException("The cluster is already started");
        }

        initialClusterSize = nodeCount;

        List<ServerRegistration> nodeRegistrations = IntStream.range(0, nodeCount)
                .mapToObj(nodeIndex -> startEmbeddedNode(testInfo, nodeIndex, nodeBootstrapConfigTemplate, nodeBootstrapConfigUpdater))
                .collect(toList());

        metaStorageAndCmgNodes = Arrays.stream(cmgNodes)
                .mapToObj(nodeRegistrations::get)
                .map(ServerRegistration::server)
                .collect(toList());

        InitParametersBuilder builder = InitParameters.builder()
                .metaStorageNodes(metaStorageAndCmgNodes)
                .clusterName(clusterConfiguration.clusterName());

        initParametersConfigurator.accept(builder);

        TestIgnitionManager.init(metaStorageAndCmgNodes.get(0), builder.build());

        for (ServerRegistration registration : nodeRegistrations) {
            assertThat(registration.registrationFuture(), willCompleteSuccessfully());
        }

        started = true;
    }

    /**
     * Starts the cluster with the given number of nodes and initializes it.
     *
     * @param nodeCount Number of nodes in the cluster.
     * @param nodeBootstrapConfigUpdater Boot configuration updater before starting the node.
     */
    public void startAndInitWithUpdateBootstrapConfig(int nodeCount, NodeBootstrapConfigUpdater nodeBootstrapConfigUpdater) {
        startAndInit(
                null,
                nodeCount,
                new int[]{0},
                clusterConfiguration.defaultNodeBootstrapConfigTemplate(),
                builder -> {},
                nodeBootstrapConfigUpdater
        );
    }

    /**
     * Starts a cluster node with the default bootstrap config template.
     *
     * @param nodeIndex Index of the node to start.
     * @return Started server and its registration future.
     */
    public ServerRegistration startEmbeddedNode(int nodeIndex) {
        return startEmbeddedNode(nodeIndex, clusterConfiguration.defaultNodeBootstrapConfigTemplate());
    }

    /**
     * Starts a cluster node.
     *
     * @param nodeIndex Index of the node to start.
     * @param nodeBootstrapConfigTemplate Bootstrap config template to use for this node.
     * @return Started server and its registration future.
     */
    public ServerRegistration startEmbeddedNode(int nodeIndex, String nodeBootstrapConfigTemplate) {
        return startEmbeddedNode(null, nodeIndex, nodeBootstrapConfigTemplate, NodeBootstrapConfigUpdater.noop());
    }

    /**
     * Starts a cluster node.
     *
     * @param testInfo Test info.
     * @param nodeIndex Index of the node to start.
     * @param nodeBootstrapConfigTemplate Bootstrap config template to use for this node.
     * @param nodeBootstrapConfigUpdater Boot configuration updater before starting the node.
     * @return Started server and its registration future.
     */
    public ServerRegistration startEmbeddedNode(
            @Nullable TestInfo testInfo,
            int nodeIndex,
            String nodeBootstrapConfigTemplate,
            NodeBootstrapConfigUpdater nodeBootstrapConfigUpdater
    ) {
        String nodeName = nodeName(nodeIndex);

        String config = nodeBootstrapConfigUpdater.update(IgniteStringFormatter.format(
                nodeBootstrapConfigTemplate,
                port(nodeIndex),
                seedAddressesString(),
                clusterConfiguration.baseClientPort() + nodeIndex,
                httpPort(nodeIndex),
                clusterConfiguration.baseHttpsPort() + nodeIndex
        ));

        if (testInfo != null && containsOverrides(testInfo, nodeIndex)) {
            config = TestIgnitionManager.applyOverridesToConfig(config, configOverrides(testInfo, nodeIndex));
        }

        IgniteServer node = clusterConfiguration.usePreConfiguredStorageProfiles()
                ? TestIgnitionManager.start(
                        nodeName,
                        config,
                        clusterConfiguration.workDir().resolve(clusterConfiguration.clusterName()).resolve(nodeName))
                : TestIgnitionManager.startWithoutPreConfiguredStorageProfiles(
                        nodeName,
                        config,
                        clusterConfiguration.workDir().resolve(clusterConfiguration.clusterName()).resolve(nodeName));

        synchronized (igniteServers) {
            setListAtIndex(igniteServers, nodeIndex, node);
        }

        CompletableFuture<Void> registrationFuture = node.waitForInitAsync().thenRun(() -> {
            synchronized (nodes) {
                setListAtIndex(nodes, nodeIndex, node.api());
            }

            if (stopped) {
                // Make sure we stop even a node that finished starting after the cluster has been stopped.
                node.shutdown();
            }
        });

        return new ServerRegistration(node, registrationFuture);
    }

    /**
     * Returns node name by index.
     *
     * @param nodeIndex Index of the node of interest.
     */
    public String nodeName(int nodeIndex) {
        return clusterConfiguration.nodeNamingStrategy().nodeName(clusterConfiguration, nodeIndex);
    }

    public int port(int nodeIndex) {
        return clusterConfiguration.basePort() + nodeIndex;
    }

    /**
     * Returns HTTP port by index.
     */
    public int httpPort(int nodeIndex) {
        return clusterConfiguration.baseHttpPort() + nodeIndex;
    }

    private String seedAddressesString() {
        int localSeedCountOverride = seedCountOverride;
        // We do this maxing because in some scenarios startAndInit() is not invoked, instead startNode() is used directly.
        int seedsCount = localSeedCountOverride > 0 ? localSeedCountOverride
                : Math.max(Math.max(initialClusterSize, nodes.size()), 1);

        return IntStream.range(0, seedsCount)
                .map(this::port)
                .mapToObj(port -> "\"localhost:" + port + '\"')
                .collect(joining(", "));
    }

    /**
     * Returns all Ignite servers.
     */
    public List<IgniteServer> servers() {
        return igniteServers;
    }

    /**
     * Returns an Ignite server by its index.
     */
    public IgniteServer server(int index) {
        return Objects.requireNonNull(igniteServers.get(index));
    }

    /**
     * Returns an Ignite node (a member of the cluster) by its index.
     */
    public Ignite node(int index) {
        return Objects.requireNonNull(nodes.get(index), "index=" + index);
    }

    /**
     * Returns an Ignite node (a member of the cluster) by its index.
     */
    public @Nullable Ignite nullableNode(int index) {
        return nodes.get(index);
    }

    /**
     * Returns a node that is not stopped and not knocked out (so it can be used to interact with the cluster).
     */
    public Ignite aliveNode() {
        return IntStream.range(0, nodes.size())
                .filter(index -> nodes.get(index) != null)
                .filter(index -> !knockedOutNodesIndices.contains(index))
                .mapToObj(nodes::get)
                .findAny()
                .orElseThrow(() -> new IllegalStateException("There is no single alive node that would not be knocked out"));
    }

    /**
     * Returns all alive nodes and their corresponding indexes.
     */
    public List<IgniteBiTuple<Integer, Ignite>> aliveNodesWithIndices() {
        return IntStream.range(0, nodes.size())
                .filter(index -> nodes.get(index) != null)
                .filter(index -> !knockedOutNodesIndices.contains(index))
                .mapToObj(index -> new IgniteBiTuple<>(index, nodes.get(index)))
                .collect(toList());
    }

    /**
     * Starts a new node with the given index.
     *
     * @param index Node index.
     * @return Started node (if the cluster is already initialized, the node is returned when it joins the cluster; if it
     *     is not initialized, the node is returned in a state in which it is ready to join the cluster).
     */
    public Ignite startNode(int index) {
        return startNode(index, clusterConfiguration.defaultNodeBootstrapConfigTemplate());
    }

    /**
     * Starts a new node with the given index.
     *
     * @param index Node index.
     * @param nodeBootstrapConfigTemplate Bootstrap config template to use for this node.
     * @return Started node (if the cluster is already initialized, the node is returned when it joins the cluster; if it
     *     is not initialized, the node is returned in a state in which it is ready to join the cluster).
     */
    public Ignite startNode(int index, String nodeBootstrapConfigTemplate) {
        ServerRegistration registration = startEmbeddedNode(index, nodeBootstrapConfigTemplate);
        assertThat("nodeIndex=" + index, registration.registrationFuture(), willSucceedIn(20, SECONDS));
        Ignite newIgniteNode = registration.server().api();

        assertEquals(newIgniteNode, nodes.get(index));

        return newIgniteNode;
    }

    /**
     * Stops a node by index.
     *
     * @param index Node index in the cluster.
     */
    public void stopNode(int index) {
        IgniteServer server = igniteServers.set(index, null);

        if (server != null) {
            server.shutdown();
        }

        nodes.set(index, null);
    }

    /**
     * Stops a node by name.
     *
     * @param name Name of the node in the cluster.
     */
    public void stopNode(String name) {
        stopNode(nodeIndex(name));
    }

    /**
     * Stops a node by index asynchronously.
     *
     * @param index Node index in the cluster.
     */
    public CompletableFuture<Void> stopNodeAsync(int index) {
        IgniteServer server = igniteServers.get(index);

        if (server != null) {
            return server.shutdownAsync().thenRun(() -> {
                igniteServers.set(index, null);

                nodes.set(index, null);
            });
        }

        return nullCompletedFuture();
    }

    /**
     * Returns index of the node.
     *
     * @param name Node name.
     * @return Node index.
     */
    public int nodeIndex(String name) {
        for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i) != null && nodes.get(i).name().equals(name)) {
                return i;
            }
        }

        throw new IllegalArgumentException("Node is not found: " + name);
    }

    /**
     * Restarts a node by index.
     *
     * @param index Node index in the cluster.
     * @return New node.
     */
    public Ignite restartNode(int index) {
        stopNode(index);

        return startNode(index);
    }

    /**
     * Returns {@link RaftGroupService} that is the leader for the given table partition.
     *
     * @param groupId Group ID for which a leader is to be found.
     * @return {@link RaftGroupService} that is the leader for the given table partition.
     * @throws InterruptedException Thrown if interrupted while waiting for the leader to be found.
     */
    public RaftGroupService leaderServiceFor(ReplicationGroupId groupId) throws InterruptedException {
        AtomicReference<RaftGroupService> serviceRef = new AtomicReference<>();

        assertTrue(
                waitForCondition(() -> {
                    RaftGroupService service = currentLeaderServiceFor(groupId);

                    serviceRef.set(service);

                    return service != null;
                }, 10_000),
                "Did not find a leader for " + groupId + " in time"
        );

        RaftGroupService result = serviceRef.get();

        assertNotNull(result);

        return result;
    }

    @Nullable
    private RaftGroupService currentLeaderServiceFor(ReplicationGroupId groupId) {
        return runningNodes()
                .map(TestWrappers::unwrapIgniteImpl)
                .flatMap(ignite -> {
                    JraftServerImpl server = (JraftServerImpl) ignite.raftManager().server();

                    Optional<RaftNodeId> maybeRaftNodeId = server.localNodes().stream()
                            .filter(nodeId -> nodeId.groupId().equals(groupId))
                            .findAny();

                    return maybeRaftNodeId.map(server::raftGroupService).stream();
                })
                .filter(service -> service.getRaftNode().isLeader())
                .findAny()
                .orElse(null);
    }

    /**
     * Returns nodes that are started and not stopped. This can include knocked out nodes.
     */
    public Stream<Ignite> runningNodes() {
        return nodes.stream().filter(Objects::nonNull);
    }

    /**
     * Returns nodes.
     */
    public List<Ignite> nodes() {
        return nodes;
    }

    /**
     * Shuts down the cluster by stopping all its nodes.
     */
    public void shutdown() {
        stopped = true;

        List<IgniteServer> serversToStop = new ArrayList<>(igniteServers);

        List<String> serverNames = serversToStop.stream()
                .filter(Objects::nonNull)
                .map(IgniteServer::name)
                .collect(toList());
        LOG.info("Shutting the cluster down [nodes={}]", serverNames);

        Collections.fill(igniteServers, null);
        Collections.fill(nodes, null);

        // TODO: IGNITE-26085 Allow stopping nodes in any order. Currently, MS nodes stop only at the last one.
        serversToStop.parallelStream()
                .filter(igniteServer -> igniteServer != null && !metaStorageAndCmgNodes.contains(igniteServer))
                .forEach(IgniteServer::shutdown);

        metaStorageAndCmgNodes.parallelStream()
                .filter(igniteServer -> igniteServer != null && serversToStop.contains(igniteServer))
                .forEach(IgniteServer::shutdown);

        metaStorageAndCmgNodes = List.of();

        LOG.info("Shut the cluster down");
    }

    /**
     * Executes an action with a {@link IgniteSql} of a node with the given index.
     *
     * @param nodeIndex Index of node on which to execute the action.
     * @param action Action to execute.
     */
    public void doInSession(int nodeIndex, Consumer<IgniteSql> action) {
        action.accept(node(nodeIndex).sql());
    }

    /**
     * Returns result of executing an action with a {@link IgniteSql} of a node with the given index.
     *
     * @param nodeIndex Index of node on which to execute the action.
     * @param action Action to execute.
     * @return Action result.
     */
    public <T> T doInSession(int nodeIndex, Function<IgniteSql, T> action) {
        return action.apply(node(nodeIndex).sql());
    }

    /**
     * Executes a SQL query on a node with the given index.
     *
     * @param nodeIndex Index of node on which to execute the query.
     * @param sql SQL query to execute.
     * @param extractor Used to extract the result from a {@link ResultSet}.
     * @return Query result.
     */
    public <T> T query(int nodeIndex, String sql, Function<ResultSet<SqlRow>, T> extractor) {
        return doInSession(nodeIndex, session -> {
            try (ResultSet<SqlRow> resultSet = session.execute(null, sql)) {
                return extractor.apply(resultSet);
            }
        });
    }

    /**
     * Simulate network partition for a chosen node. More precisely, drop all messages sent to it by other cluster members.
     *
     * <p>WARNING: this should only be used carefully because 'drop all messages to a node' might break some invariants
     * after the 'connectivity' is restored with {@link #removeNetworkPartitionOf(int)}. Only use this method if you
     * know what you are doing! Prefer {@link #stopNode(int)}.
     *
     * @param nodeIndex Index of the node messages to which need to be dropped.
     */
    public void simulateNetworkPartitionOf(int nodeIndex) {
        Ignite recipient = node(nodeIndex);

        runningNodes()
                .filter(node -> node != recipient)
                .map(TestWrappers::unwrapIgniteImpl)
                .forEach(sourceNode -> {
                    sourceNode.dropMessages(
                            new AddCensorshipByRecipientConsistentId(recipient.name(), sourceNode.dropMessagesPredicate())
                    );
                });

        knockedOutNodesIndices.add(nodeIndex);

        LOG.info("Knocked out node " + nodeIndex + " with an artificial network partition");
    }

    /**
     * Removes the simulated 'network partition' for the given node.
     *
     * @param nodeIndex Index of the node.
     * @see #simulateNetworkPartitionOf(int)
     */
    public void removeNetworkPartitionOf(int nodeIndex) {
        Ignite receiver = node(nodeIndex);

        runningNodes()
                .filter(node -> node != receiver)
                .map(TestWrappers::unwrapIgniteImpl)
                .forEach(ignite -> {
                    var censor = (AddCensorshipByRecipientConsistentId) ignite.dropMessagesPredicate();

                    assertNotNull(censor);

                    if (censor.prevPredicate == null) {
                        ignite.stopDroppingMessages();
                    } else {
                        ignite.dropMessages(censor.prevPredicate);
                    }
                });

        knockedOutNodesIndices.remove(nodeIndex);

        LOG.info("Reanimated node " + nodeIndex + " by removing an artificial network partition");
    }

    /**
     * Transfers leadership over a replication group to a node identified by the given index.
     *
     * @param nodeIndex Node index of the new leader.
     * @param groupId ID of the replication group.
     * @throws InterruptedException If interrupted while waiting.
     */
    public void transferLeadershipTo(int nodeIndex, ReplicationGroupId groupId) throws InterruptedException {
        String nodeConsistentId = unwrapIgniteImpl(node(nodeIndex)).node().name();

        int maxAttempts = 3;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            boolean transferred = tryTransferLeadershipTo(nodeConsistentId, groupId);

            if (transferred) {
                break;
            }

            if (attempt < maxAttempts) {
                LOG.info("Did not transfer leadership after {} attempts, going to retry...", attempt);
            } else {
                fail("Did not transfer leadership in time after " + maxAttempts + " attempts");
            }
        }
    }

    private boolean tryTransferLeadershipTo(String targetLeaderConsistentId, ReplicationGroupId groupId) throws InterruptedException {
        NodeImpl leaderBeforeTransfer = (NodeImpl) leaderServiceFor(groupId).getRaftNode();

        initiateLeadershipTransferTo(targetLeaderConsistentId, leaderBeforeTransfer);

        BooleanSupplier leaderTransferred = () -> {
            PeerId leaderId = leaderBeforeTransfer.getLeaderId();
            return leaderId != null && leaderId.getConsistentId().equals(targetLeaderConsistentId);
        };

        return waitForCondition(leaderTransferred, 10_000);
    }

    private static void initiateLeadershipTransferTo(String targetLeaderConsistentId, NodeImpl leaderBeforeTransfer) {
        long startedMillis = System.currentTimeMillis();

        while (true) {
            Status status = leaderBeforeTransfer.transferLeadershipTo(new PeerId(targetLeaderConsistentId));

            if (status.getRaftError() != RaftError.EBUSY) {
                break;
            }

            if (System.currentTimeMillis() - startedMillis > 10_000) {
                throw new IllegalStateException("Could not initiate leadership transfer to " + targetLeaderConsistentId + " in time");
            }
        }
    }

    /**
     * Transfers primary replica of given replication group to the node with given index.
     *
     * @param nodeIndex Destination node index.
     * @param groupId ID of the replication group.
     */
    public void transferPrimaryTo(int nodeIndex, ReplicationGroupId groupId) throws InterruptedException {
        String proposedPrimaryName = node(nodeIndex).name();

        if (!proposedPrimaryName.equals(getPrimaryReplicaName(groupId))) {

            String newPrimaryName = NodeUtils.transferPrimary(
                    runningNodes().map(TestWrappers::unwrapIgniteImpl).collect(toList()),
                    groupId,
                    proposedPrimaryName
            );

            assertEquals(proposedPrimaryName, newPrimaryName);
        }
    }

    private @Nullable String getPrimaryReplicaName(ReplicationGroupId groupId) {
        IgniteImpl node = unwrapIgniteImpl(aliveNode());

        CompletableFuture<ReplicaMeta> primary = node.placementDriver()
                .awaitPrimaryReplica(groupId, node.clockService().now(), 30, SECONDS);

        assertThat(primary, willCompleteSuccessfully());

        @Nullable ReplicaMeta replicaMeta = primary.join();
        return replicaMeta != null ? replicaMeta.getLeaseholder() : null;
    }

    /**
     * Returns the ID of the sole partition that exists in the cluster or throws if there are less than one
     * or more than one partitions.
     */
    public ReplicationGroupId solePartitionId(String zoneName, String tableName) {
        IgniteImpl node = unwrapIgniteImpl(aliveNode());

        Catalog catalog = node.catalogManager().catalog(node.catalogManager().latestCatalogVersion());

        CatalogZoneDescriptor zoneDescriptor = catalog.zone(zoneName.toUpperCase());
        CatalogTableDescriptor tableDescriptor = catalog.table(SqlCommon.DEFAULT_SCHEMA_NAME, tableName.toUpperCase());

        List<? extends ReplicationGroupId> replicationGroupIds = colocationEnabled()
                ? zonePartitionIds(unwrapIgniteImpl(aliveNode()), zoneDescriptor.id())
                : tablePartitionIds(unwrapIgniteImpl(aliveNode()), tableDescriptor.id());

        assertThat(replicationGroupIds.size(), is(1));

        return replicationGroupIds.get(0);
    }

    /**
     * Sets number of seeds to use instead of initialClusterSize.
     *
     * @param newOverride New override value.
     */
    public void overrideSeedsCount(int newOverride) {
        seedCountOverride = newOverride;
    }

    private static class AddCensorshipByRecipientConsistentId implements BiPredicate<String, NetworkMessage> {
        private final String recipientName;
        @Nullable
        private final BiPredicate<String, NetworkMessage> prevPredicate;

        private AddCensorshipByRecipientConsistentId(String recipientName, @Nullable BiPredicate<String, NetworkMessage> prevPredicate) {
            this.recipientName = recipientName;
            this.prevPredicate = prevPredicate;
        }

        @Override
        public boolean test(String recipientConsistentId, NetworkMessage networkMessage) {
            return Objects.equals(recipientConsistentId, recipientName)
                    || (prevPredicate != null && prevPredicate.test(recipientConsistentId, networkMessage));
        }
    }

    /** {@link IgniteServer} and future that completes when the server gets started, joins and gets registered with a Cluster. */
    public static class ServerRegistration {
        private final IgniteServer server;
        private final CompletableFuture<Void> registrationFuture;

        public ServerRegistration(IgniteServer server, CompletableFuture<Void> registrationFuture) {
            this.server = server;
            this.registrationFuture = registrationFuture;
        }

        /** Returns IgniteServer. */
        public IgniteServer server() {
            return server;
        }

        /**
         * Returns a future that gets completed when the server gets started, joins and gets registered with the Cluster which starts it.
         */
        public CompletableFuture<Void> registrationFuture() {
            return registrationFuture;
        }
    }
}
