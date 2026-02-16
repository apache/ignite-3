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

package org.apache.ignite.internal.runner.app.client;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.replicator.configuration.ReplicationConfigurationSchema.DEFAULT_IDLE_SAFE_TIME_PROP_DURATION;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServer;
import org.apache.ignite.InitParameters;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.BroadcastExecution;
import org.apache.ignite.compute.BroadcastJobTarget;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.internal.raft.configuration.RaftConfigurationSchema;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.CancellationToken;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.sql.IgniteSql;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Thin client integration test base class.
 */
@SuppressWarnings("ZeroLengthArrayAllocation")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(WorkDirectoryExtension.class)
public abstract class ItAbstractThinClientTest extends BaseIgniteAbstractTest {
    protected static final String ZONE_NAME = "TEST_ZONE";

    protected static final String TABLE_NAME = "TBL1";

    protected static final String COLUMN_KEY = "key";

    protected static final String COLUMN_VAL = "val";

    protected static final int PARTITIONS = 10;

    private final Map<String, String> nodesBootstrapCfg = new LinkedHashMap<>();

    private final List<Ignite> startedNodes = new ArrayList<>();

    private IgniteClient client;
    private List<IgniteServer> nodes;

    /**
     * Before all.
     */
    @BeforeAll
    void beforeAll(TestInfo testInfo, @WorkDirectory Path workDir) throws InterruptedException {
        for (int i = 0; i < nodes(); i++) {
            String nodeName = testNodeName(testInfo, 3344 + i);

            nodesBootstrapCfg.put(
                    nodeName,
                    "ignite {\n"
                            + "  network.port: " + (3344 + i) + ",\n"
                            + "  network.nodeFinder.netClusterNodes: [ \"localhost:3344\" ]\n"
                            + (i == 1 ? ("  clientConnector.sendServerExceptionStackTraceToClient: true\n"
                            + "  clientConnector.metricsEnabled: true\n") : "")
                            + "  clientConnector.port: " + (10800 + i) + ",\n"
                            + "  rest.port: " + (10300 + i) + ",\n"
                            + "  compute.threadPoolSize: 1,\n"
                            + "  raft.retryTimeoutMillis: " + raftTimeoutMillis()
                            + "}"
            );
        }

        nodes = nodesBootstrapCfg.entrySet().stream()
                .map(e -> TestIgnitionManager.start(e.getKey(), e.getValue(), workDir.resolve(e.getKey())))
                .collect(toList());

        IgniteServer metaStorageNode = nodes.get(0);

        InitParameters initParameters = InitParameters.builder()
                .metaStorageNodes(metaStorageNode)
                .clusterName("cluster")
                .clusterConfiguration("ignite.replication.idleSafeTimePropagationDurationMillis: " + idleSafeTimePropagationDuration())
                .build();
        TestIgnitionManager.init(metaStorageNode, initParameters);

        for (IgniteServer node : nodes) {
            assertThat(node.waitForInitAsync(), willCompleteSuccessfully());

            startedNodes.add(node.api());
        }

        IgniteSql sql = startedNodes.get(0).sql();

        sql.execute("CREATE ZONE " + ZONE_NAME + " (REPLICAS " + replicas() + ", PARTITIONS " + PARTITIONS + ") STORAGE PROFILES ['"
                + DEFAULT_STORAGE_PROFILE + "']");
        sql.execute("CREATE TABLE " + TABLE_NAME + "("
                + COLUMN_KEY + " INT PRIMARY KEY, " + COLUMN_VAL + " VARCHAR) ZONE TEST_ZONE");

        client = IgniteClient.builder()
                .addresses(getClientAddresses().toArray(new String[0]))
                .operationTimeout(15_000)
                .build();

        assertTrue(IgniteTestUtils.waitForCondition(() -> client.connections().size() == nodes(), 3000));
    }

    /**
     * After each.
     */
    @AfterAll
    void afterAll() throws Exception {
        var closeables = new ArrayList<AutoCloseable>();

        closeables.add(client);

        nodes.stream()
                .map(node -> (AutoCloseable) node::shutdown)
                .forEach(closeables::add);

        IgniteUtils.closeAll(closeables);
    }

    String getNodeAddress() {
        return getClientAddresses().get(0);
    }

    List<String> getClientAddresses() {
        return getClientAddresses(startedNodes);
    }

    /**
     * Gets client connector addresses for the specified nodes.
     *
     * @param nodes Nodes.
     * @return List of client addresses.
     */
    public static List<String> getClientAddresses(List<Ignite> nodes) {
        return getClientPorts(nodes).stream()
                .map(port -> "127.0.0.1" + ":" + port)
                .collect(toList());
    }

    List<Integer> getClientPorts() {
        return getClientPorts(startedNodes);
    }

    private static List<Integer> getClientPorts(List<Ignite> nodes) {
        return nodes.stream()
                .map(ignite -> unwrapIgniteImpl(ignite).clientAddress().port())
                .collect(toList());
    }

    protected long idleSafeTimePropagationDuration() {
        return DEFAULT_IDLE_SAFE_TIME_PROP_DURATION;
    }

    protected long raftTimeoutMillis() {
        return new RaftConfigurationSchema().retryTimeoutMillis;
    }

    protected IgniteClient client() {
        return client;
    }

    protected IgniteServer ignite(int idx) {
        return nodes.get(idx);
    }

    protected Ignite server() {
        return startedNodes.get(0);
    }

    protected Ignite server(int idx) {
        return startedNodes.get(idx);
    }

    protected Ignite server(ClusterNode node) {
        return IntStream.range(0, nodes()).mapToObj(this::server).filter(n -> n.name().equals(node.name())).findFirst().orElseThrow();
    }

    protected ClusterNode node(int idx) {
        return sortedNodes().get(idx);
    }

    protected int replicas() {
        return 1;
    }

    protected int nodes() {
        return 2;
    }

    protected List<ClusterNode> sortedNodes() {
        return client.cluster().nodes().stream()
                .sorted(Comparator.comparing(ClusterNode::name))
                .collect(toList());
    }

    /**
     * Submits the job for execution, verifies that the execution future completes successfully and returns an execution object.
     *
     * @param <T> Job argument (T)ype.
     * @param <R> Job (R)esult type.
     * @param target Execution target.
     * @param descriptor Job descriptor.
     * @param arg Argument of the job.
     * @return Job execution object.
     */
    protected <T, R> JobExecution<R> submit(
            JobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg
    ) {
        return submit(target, descriptor, null, arg);
    }

    protected <T, R> JobExecution<R> submit(
            JobTarget target,
            JobDescriptor<T, R> descriptor,
            @Nullable CancellationToken cancellationToken,
            @Nullable T arg
    ) {
        //noinspection resource (closed in afterAll)
        CompletableFuture<JobExecution<R>> executionFut = client().compute().submitAsync(target, descriptor, arg, cancellationToken);
        assertThat(executionFut, willCompleteSuccessfully());
        return executionFut.join();
    }

    protected <T, R> BroadcastExecution<R> submit(
            Set<ClusterNode> nodes,
            JobDescriptor<T, R> descriptor,
            @Nullable T arg
    ) {
        return submit(nodes, descriptor, null, arg);
    }

    protected <T, R> BroadcastExecution<R> submit(
            Set<ClusterNode> nodes,
            JobDescriptor<T, R> descriptor,
            @Nullable CancellationToken cancellationToken,
            @Nullable T arg
    ) {
        //noinspection resource (closed in afterAll)
        CompletableFuture<BroadcastExecution<R>> executionFut = client().compute().submitAsync(
                BroadcastJobTarget.nodes(nodes),
                descriptor,
                arg,
                cancellationToken
        );
        assertThat(executionFut, willCompleteSuccessfully());
        return executionFut.join();
    }

    /**
     * Test class.
     */
    protected static class TestPojo {
        public TestPojo() {
            // No-op.
        }

        public TestPojo(int key) {
            this.key = key;
        }

        public int key;

        public String val;

        public int getKey() {
            return key;
        }

        public String getVal() {
            return val;
        }
    }
}
