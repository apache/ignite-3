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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.util.concurrent.ConcurrentHashSet;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Session;
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

    private static final String CONNECT_NODE_ADDR = "\"localhost:" + BASE_PORT + '\"';

    /** Timeout for SQL queries (in milliseconds). */
    private static final int QUERY_TIMEOUT_MS = 10_000;

    /** Default nodes bootstrap configuration pattern. */
    private static final String DEFAULT_NODE_BOOTSTRAP_CFG = "{\n"
            + "  \"network\": {\n"
            + "    \"port\":{},\n"
            + "    \"nodeFinder\":{\n"
            + "      \"netClusterNodes\": [ {} ]\n"
            + "    }\n"
            + "  }\n"
            + "}";

    private final TestInfo testInfo;

    private final Path workDir;

    private final String defaultNodeBootstrapConfigTemplate;

    /** Cluster nodes. */
    private final List<IgniteImpl> nodes = new CopyOnWriteArrayList<>();

    private volatile boolean started = false;

    private volatile boolean stopped = false;

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

    /**
     * Starts the cluster with the given number of nodes and initializes it.
     *
     * @param nodeCount Number of nodes in the cluster.
     */
    public void startAndInit(int nodeCount) {
        if (started) {
            throw new IllegalStateException("The cluster is already started");
        }

        List<CompletableFuture<IgniteImpl>> futures = IntStream.range(0, nodeCount)
                .mapToObj(this::startClusterNode)
                .collect(toList());

        String metaStorageAndCmgNodeName = testNodeName(testInfo, 0);

        InitParameters initParameters = InitParameters.builder()
                .setNodeName(metaStorageAndCmgNodeName)
                .setMetaStorageNodeNames(List.of(metaStorageAndCmgNodeName))
                .setClusterName("cluster")
                .build();

        IgnitionManager.init(initParameters);

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
    public CompletableFuture<IgniteImpl> startClusterNode(int nodeIndex) {
        return startClusterNode(nodeIndex, defaultNodeBootstrapConfigTemplate);
    }

    /**
     * Starts a cluster node and returns its startup future.
     *
     * @param nodeIndex Index of the nodex to start.
     * @param nodeBootstrapConfigTemplate Bootstrap config template to use for this node.
     * @return Future that will be completed when the node starts.
     */
    public CompletableFuture<IgniteImpl> startClusterNode(int nodeIndex, String nodeBootstrapConfigTemplate) {
        String nodeName = testNodeName(testInfo, nodeIndex);

        String config = IgniteStringFormatter.format(nodeBootstrapConfigTemplate, BASE_PORT + nodeIndex, CONNECT_NODE_ADDR);

        return IgnitionManager.start(nodeName, config, workDir.resolve(nodeName))
                .thenApply(IgniteImpl.class::cast)
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
            newIgniteNode = startClusterNode(index, nodeBootstrapConfigTemplate).get(10, TimeUnit.SECONDS);
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
     * @param tablePartitionId Table partition ID for which a leader is to be found.
     * @return {@link RaftGroupService} that is the leader for the given table partition.
     * @throws InterruptedException Thrown if interrupted while waiting for the leader to be found.
     */
    public RaftGroupService leaderServiceFor(TablePartitionId tablePartitionId) throws InterruptedException {
        AtomicReference<RaftGroupService> serviceRef = new AtomicReference<>();

        assertTrue(
                waitForCondition(() -> {
                    RaftGroupService service = currentLeaderServiceFor(tablePartitionId);

                    serviceRef.set(service);

                    return service != null;
                }, 10_000),
                "Did not find a leader for " + tablePartitionId + " in time"
        );

        RaftGroupService result = serviceRef.get();

        assertNotNull(result);

        return result;
    }

    @Nullable
    private RaftGroupService currentLeaderServiceFor(TablePartitionId tablePartitionId) {
        return runningNodes()
                .map(IgniteImpl.class::cast)
                .flatMap(ignite -> {
                    JraftServerImpl server = (JraftServerImpl) ignite.raftManager().server();

                    Optional<RaftNodeId> maybeRaftNodeId = server.localNodes().stream()
                            .filter(nodeId -> nodeId.groupId().equals(tablePartitionId))
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
     * Opens a {@link Session} (that can be used to execute SQL queries) through a node with the given index.
     *
     * @param nodeIndex Index of the node on which to open a session.
     * @return A session.
     */
    public Session openSession(int nodeIndex) {
        return node(nodeIndex).sql()
                .sessionBuilder()
                .defaultSchema("PUBLIC")
                .defaultQueryTimeout(QUERY_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .build();
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

        nodesToStop.forEach(node -> IgnitionManager.stop(node.name()));
    }

    /**
     * Knocks out a node so that it stops receiving messages from other nodes of the cluster. To bring a node back,
     * {@link #reanimateNode(int, NodeKnockout)} should be used.
     */
    public void knockOutNode(int nodeIndex, NodeKnockout knockout) {
        knockout.knockOutNode(nodeIndex, this);

        knockedOutNodesIndices.add(nodeIndex);
    }

    /**
     * Reanimates a knocked-out node so that it starts receiving messages from other nodes of the cluster again. This nullifies the
     * effect of {@link #knockOutNode(int, NodeKnockout)}.
     */
    public void reanimateNode(int nodeIndex, NodeKnockout knockout) {
        knockout.reanimateNode(nodeIndex, this);

        knockedOutNodesIndices.remove(nodeIndex);
    }

    /**
     * Executes an action with a {@link Session} opened via a node with the given index.
     *
     * @param nodeIndex Index of node on which to execute the action.
     * @param action Action to execute.
     */
    public void doInSession(int nodeIndex, Consumer<Session> action) {
        try (Session session = openSession(nodeIndex)) {
            action.accept(session);
        }
    }

    /**
     * Returns result of executing an action with a {@link Session} opened via a node with the given index.
     *
     * @param nodeIndex Index of node on which to execute the action.
     * @param action Action to execute.
     * @return Action result.
     */
    public <T> T doInSession(int nodeIndex, Function<Session, T> action) {
        try (Session session = openSession(nodeIndex)) {
            return action.apply(session);
        }
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
     * A way to make a node be separated from a cluster and stop receiving updates.
     */
    public enum NodeKnockout {
        /** Stop a node to knock it out. */
        STOP {
            @Override
            void knockOutNode(int nodeIndex, Cluster cluster) {
                cluster.stopNode(nodeIndex);
            }

            @Override
            void reanimateNode(int nodeIndex, Cluster cluster) {
                cluster.startNode(nodeIndex);
            }
        },
        /** Emulate a network partition so that messages to the knocked-out node are dropped. */
        PARTITION_NETWORK {
            @Override
            void knockOutNode(int nodeIndex, Cluster cluster) {
                IgniteImpl recipient = cluster.node(nodeIndex);

                cluster.runningNodes()
                        .filter(node -> node != recipient)
                        .forEach(sourceNode -> {
                            sourceNode.dropMessages(
                                    new AddCensorshipByRecipientConsistentId(recipient.name(), sourceNode.dropMessagesPredicate())
                            );
                        });

                LOG.info("Knocked out node " + nodeIndex + " with an artificial network partition");
            }

            @Override
            void reanimateNode(int nodeIndex, Cluster cluster) {
                IgniteImpl receiver = cluster.node(nodeIndex);

                cluster.runningNodes()
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

                LOG.info("Reanimated node " + nodeIndex + " by removing an artificial network partition");
            }
        };

        /**
         * Knocks out a node so that it stops receiving messages from other nodes of the cluster. To bring a node back,
         * {@link #reanimateNode(int, Cluster)} should be used.
         */
        abstract void knockOutNode(int nodeIndex, Cluster cluster);

        /**
         * Reanimates a knocked-out node so that it starts receiving messages from other nodes of the cluster again. This nullifies the
         * effect of {@link #knockOutNode(int, Cluster)}.
         */
        abstract void reanimateNode(int nodeIndex, Cluster cluster);
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
