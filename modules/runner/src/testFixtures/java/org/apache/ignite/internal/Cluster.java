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

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.util.concurrent.ConcurrentHashSet;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.TestInfo;

/**
 * Cluster of nodes used for testing.
 */
@SuppressWarnings("resource")
public class Cluster {
    private static final IgniteLogger LOG = Loggers.forClass(Cluster.class);

    /** Base port number. */
    private static final int BASE_PORT = 3344;

    public static final int BASE_CLIENT_PORT = 10800;

    public static final int BASE_HTTP_PORT = 10300;

    private static final int BASE_HTTPS_PORT = 10400;

    /** Timeout for SQL queries (in milliseconds). */
    private static final int QUERY_TIMEOUT_MS = 10_000;

    /** Default nodes bootstrap configuration pattern. */
    private static final String DEFAULT_NODE_BOOTSTRAP_CFG = "{\n"
            + "  \"network\": {\n"
            + "    \"port\":{},\n"
            + "    \"nodeFinder\":{\n"
            + "      \"netClusterNodes\": [ {} ]\n"
            + "    }\n"
            + "  },\n"
            + "  clientConnector: { port:{} }\n"
            + "  rest: {\n"
            + "    port: {},\n"
            + "    ssl.port: {}\n"
            + "  }\n"
            + "}";

    private final TestInfo testInfo;

    private final Path workDir;

    private final String defaultNodeBootstrapConfigTemplate;

    /** Cluster nodes. */
    private final List<IgniteImpl> nodes = new CopyOnWriteArrayList<>();

    private volatile boolean started = false;

    private volatile boolean stopped = false;

    /** Number of nodes in the cluster on first startAndInit() [if it was invoked]. */
    private volatile int initialClusterSize;

    /** Indices of nodes that have been knocked out. */
    private final Set<Integer> knockedOutNodesIndices = new ConcurrentHashSet<>();

    /**
     * Creates a new cluster with a default bootstrap config.
     */
    public Cluster(TestInfo testInfo, Path workDir) {
        this(testInfo, workDir, DEFAULT_NODE_BOOTSTRAP_CFG);
    }

    /**
     * Creates a new cluster with the given bootstrap config.
     */
    public Cluster(TestInfo testInfo, Path workDir, String defaultNodeBootstrapConfigTemplate) {
        this.testInfo = testInfo;
        this.workDir = workDir;
        this.defaultNodeBootstrapConfigTemplate = defaultNodeBootstrapConfigTemplate;
    }

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
        startAndInit(nodeCount, cmgNodes, builder -> {});
    }

    /**
     * Starts the cluster with the given number of nodes and initializes it.
     *
     * @param nodeCount Number of nodes in the cluster.
     * @param initParametersConfigurator Configure {@link InitParameters} before initializing the cluster.
     */
    public void startAndInit(int nodeCount, Consumer<InitParametersBuilder> initParametersConfigurator) {
        startAndInit(nodeCount, new int[]{0}, initParametersConfigurator);
    }

    /**
     * Starts the cluster with the given number of nodes and initializes it.
     *
     * @param nodeCount Number of nodes in the cluster.
     * @param cmgNodes Indices of CMG nodes.
     * @param initParametersConfigurator Configure {@link InitParameters} before initializing the cluster.
     */
    public void startAndInit(int nodeCount, int[] cmgNodes, Consumer<InitParametersBuilder> initParametersConfigurator) {
        startAndInit(nodeCount, cmgNodes, defaultNodeBootstrapConfigTemplate, initParametersConfigurator);
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
        startAndInit(nodeCount, new int[] { 0 }, nodeBootstrapConfigTemplate, initParametersConfigurator);
    }

    /**
     * Starts the cluster with the given number of nodes and initializes it.
     *
     * @param nodeCount Number of nodes in the cluster.
     * @param cmgNodes Indices of CMG nodes.
     * @param nodeBootstrapConfigTemplate Node bootstrap config template to be used for each node started
     *     with this call.
     * @param initParametersConfigurator Configure {@link InitParameters} before initializing the cluster.
     */
    private void startAndInit(
            int nodeCount,
            int[] cmgNodes,
            String nodeBootstrapConfigTemplate,
            Consumer<InitParametersBuilder> initParametersConfigurator
    ) {
        if (started) {
            throw new IllegalStateException("The cluster is already started");
        }

        initialClusterSize = nodeCount;

        List<CompletableFuture<IgniteImpl>> futures = IntStream.range(0, nodeCount)
                .mapToObj(nodeIndex -> startNodeAsync(nodeIndex, nodeBootstrapConfigTemplate))
                .collect(toList());

        List<String> metaStorageAndCmgNodeNames = Arrays.stream(cmgNodes).mapToObj(i -> testNodeName(testInfo, i)).collect(toList());

        InitParametersBuilder builder = InitParameters.builder()
                .destinationNodeName(metaStorageAndCmgNodeNames.get(0))
                .metaStorageNodeNames(metaStorageAndCmgNodeNames)
                .clusterName("cluster");

        initParametersConfigurator.accept(builder);

        TestIgnitionManager.init(builder.build());

        for (CompletableFuture<IgniteImpl> future : futures) {
            assertThat(future, willCompleteSuccessfully());
        }

        started = true;
    }

    /**
     * Starts a cluster node with the default bootstrap config template and returns its startup future.
     *
     * @param nodeIndex Index of the node to start.
     * @return Future that will be completed when the node starts.
     */
    public CompletableFuture<IgniteImpl> startNodeAsync(int nodeIndex) {
        return startNodeAsync(nodeIndex, defaultNodeBootstrapConfigTemplate);
    }

    /**
     * Starts a cluster node and returns its startup future.
     *
     * @param nodeIndex Index of the nodex to start.
     * @param nodeBootstrapConfigTemplate Bootstrap config template to use for this node.
     * @return Future that will be completed when the node starts.
     */
    public CompletableFuture<IgniteImpl> startNodeAsync(int nodeIndex, String nodeBootstrapConfigTemplate) {
        String nodeName = testNodeName(testInfo, nodeIndex);

        String config = IgniteStringFormatter.format(
                nodeBootstrapConfigTemplate,
                BASE_PORT + nodeIndex,
                seedAddressesString(),
                BASE_CLIENT_PORT + nodeIndex,
                BASE_HTTP_PORT + nodeIndex,
                BASE_HTTPS_PORT + nodeIndex
        );

        return TestIgnitionManager.start(nodeName, config, workDir.resolve(nodeName))
                .thenApply(IgniteImpl.class::cast)
                .thenCompose(ignite -> ignite.catalogManager().catalogInitializationFuture().thenApply(ignored -> ignite))
                .thenApply(ignite -> {
                    synchronized (nodes) {
                        while (nodes.size() < nodeIndex) {
                            nodes.add(null);
                        }

                        if (nodes.size() < nodeIndex + 1) {
                            nodes.add(ignite);
                        } else {
                            nodes.set(nodeIndex, ignite);
                        }
                    }

                    if (stopped) {
                        // Make sure we stop even a node that finished starting after the cluster has been stopped.

                        IgnitionManager.stop(ignite.name());
                    }

                    return ignite;
                });
    }

    private String seedAddressesString() {
        // We do this maxing because in some scenarios startAndInit() is not invoked, instead startNode() is used directly.
        int seedsCount = Math.max(Math.max(initialClusterSize, nodes.size()), 1);

        return IntStream.range(0, seedsCount)
                .map(index -> BASE_PORT + index)
                .mapToObj(port -> "\"localhost:" + port + '\"')
                .collect(joining(", "));
    }

    /**
     * Returns an Ignite node (a member of the cluster) by its index.
     */
    public IgniteImpl node(int index) {
        return Objects.requireNonNull(nodes.get(index));
    }

    /**
     * Returns a node that is not stopped and not knocked out (so it can be used to interact with the cluster).
     */
    public IgniteImpl aliveNode() {
        return IntStream.range(0, nodes.size())
                .filter(index -> nodes.get(index) != null)
                .filter(index -> !knockedOutNodesIndices.contains(index))
                .mapToObj(nodes::get)
                .findAny()
                .orElseThrow(() -> new IllegalStateException("There is no single alive node that would not be knocked out"));
    }

    /**
     * Starts a new node with the given index.
     *
     * @param index Node index.
     * @return Started node (if the cluster is already initialized, the node is returned when it joins the cluster; if it
     *     is not initialized, the node is returned in a state in which it is ready to join the cluster).
     */
    public IgniteImpl startNode(int index) {
        return startNode(index, defaultNodeBootstrapConfigTemplate);
    }

    /**
     * Starts a new node with the given index.
     *
     * @param index Node index.
     * @param nodeBootstrapConfigTemplate Bootstrap config template to use for this node.
     * @return Started node (if the cluster is already initialized, the node is returned when it joins the cluster; if it
     *     is not initialized, the node is returned in a state in which it is ready to join the cluster).
     */
    public IgniteImpl startNode(int index, String nodeBootstrapConfigTemplate) {
        IgniteImpl newIgniteNode;

        try {
            newIgniteNode = startNodeAsync(index, nodeBootstrapConfigTemplate).get(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new RuntimeException(e);
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }

        assertEquals(newIgniteNode, nodes.get(index));

        return newIgniteNode;
    }

    private void checkNodeIndex(int index) {
        if (index < 0) {
            throw new IllegalArgumentException("Index cannot be negative");
        }
        if (index >= nodes.size()) {
            throw new IllegalArgumentException("Cluster only contains " + nodes.size() + " nodes, but node with index "
                    + index + " was tried to be accessed");
        }
    }

    /**
     * Stops a node by index.
     *
     * @param index Node index in the cluster.
     */
    public void stopNode(int index) {
        checkNodeIndex(index);

        IgnitionManager.stop(nodes.get(index).name());

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
    public IgniteImpl restartNode(int index) {
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
                .map(IgniteImpl.class::cast)
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
    public Stream<IgniteImpl> runningNodes() {
        return nodes.stream().filter(Objects::nonNull);
    }

    /**
     * Shuts down the  cluster by stopping all its nodes.
     */
    public void shutdown() {
        stopped = true;

        List<IgniteImpl> nodesToStop;

        synchronized (nodes) {
            nodesToStop = runningNodes().collect(toList());
        }

        nodesToStop.parallelStream().forEach(node -> IgnitionManager.stop(node.name()));
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
        IgniteImpl recipient = node(nodeIndex);

        runningNodes()
                .filter(node -> node != recipient)
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
        IgniteImpl receiver = node(nodeIndex);

        runningNodes()
                .filter(node -> node != receiver)
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
     * Transfers leadsership over a replication group to a node identified by the given index.
     *
     * @param nodeIndex Node index of the new leader.
     * @param groupId ID of the replication group.
     * @throws InterruptedException If interrupted while waiting.
     */
    public void transferLeadershipTo(int nodeIndex, ReplicationGroupId groupId) throws InterruptedException {
        String nodeConsistentId = node(nodeIndex).node().name();

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
     * Returns the ID of the sole table partition that exists in the cluster or throws if there are less than one
     * or more than one partitions.
     */
    public TablePartitionId solePartitionId() {
        List<TablePartitionId> tablePartitionIds = ReplicationGroupsUtils.tablePartitionIds(aliveNode());

        assertThat(tablePartitionIds.size(), is(1));

        return tablePartitionIds.get(0);
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
}
