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

import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIMEM_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_ROCKSDB_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_TEST_PROFILE_NAME;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Abstract integration test that starts and stops a cluster per test method.
 */
@SuppressWarnings("ALL")
@ExtendWith(WorkDirectoryExtension.class)
public abstract class ClusterPerTestIntegrationTest extends BaseIgniteAbstractTest {
    private static final IgniteLogger LOG = Loggers.forClass(ClusterPerTestIntegrationTest.class);

    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG_TEMPLATE = "{\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder.netClusterNodes: [ {} ]\n"
            + "  },\n"
            + "  storage.profiles: {"
            + "        " + DEFAULT_TEST_PROFILE_NAME + ".engine: test, "
            + "        " + DEFAULT_AIPERSIST_PROFILE_NAME + ".engine: aipersist, "
            + "        " + DEFAULT_AIMEM_PROFILE_NAME + ".engine: aimem, "
            + "        " + DEFAULT_ROCKSDB_PROFILE_NAME + ".engine: rocksdb"
            + "  },\n"
            + "  clientConnector.port: {},\n"
            + "  rest.port: {},\n"
            + "  compute.threadPoolSize: 1\n"
            + "}";

    /** Template for node bootstrap config with Scalecube settings for fast failure detection. */
    public static final String FAST_FAILURE_DETECTION_NODE_BOOTSTRAP_CFG_TEMPLATE = "{\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder: {\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    },\n"
            + "    membership: {\n"
            + "      membershipSyncInterval: 1000,\n"
            + "      failurePingInterval: 500,\n"
            + "      scaleCube: {\n"
            + "        membershipSuspicionMultiplier: 1,\n"
            + "        failurePingRequestMembers: 1,\n"
            + "        gossipInterval: 10\n"
            + "      },\n"
            + "    }\n"
            + "  },\n"
            + "  clientConnector: { port:{} }, \n"
            + "  rest.port: {}\n"
            + "}";

    /** Template for node bootstrap config with Scalecube settings for a disabled failure detection. */
    protected static final String DISABLED_FAILURE_DETECTION_NODE_BOOTSTRAP_CFG_TEMPLATE = "{\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder: {\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    },\n"
            + "    membership: {\n"
            + "      failurePingInterval: 1000000000\n"
            + "    }\n"
            + "  },\n"
            + "  clientConnector: { port:{} },\n"
            + "  rest.port: {}\n"
            + "}";

    protected Cluster cluster;

    /** Work directory. */
    @WorkDirectory
    protected Path workDir;

    /**
     * Invoked before each test starts.
     *
     * @param testInfo Test information oject.
     * @throws Exception If failed.
     */
    @BeforeEach
    public void setup(TestInfo testInfo) throws Exception {
        cluster = new Cluster(testInfo, workDir, getNodeBootstrapConfigTemplate());

        if (initialNodes() > 0) {
            cluster.startAndInit(initialNodes(), cmgMetastoreNodes(), this::customizeInitParameters);
        }
    }

    @AfterEach
    @Timeout(60)
    public void tearDown() {
        cluster.shutdown();
    }

    /**
     * Returns count of nodes in the Ignite cluster started before each test.
     *
     * @return Count of nodes in initial cluster.
     */
    protected int initialNodes() {
        return 3;
    }

    protected int[] cmgMetastoreNodes() {
        return new int[] { 0 };
    }

    protected void customizeInitParameters(InitParametersBuilder builder) {
        // No-op.
    }

    /**
     * Returns node bootstrap config template.
     *
     * @return Node bootstrap config template.
     */
    protected String getNodeBootstrapConfigTemplate() {
        return NODE_BOOTSTRAP_CFG_TEMPLATE;
    }

    /**
     * Starts an Ignite node with the given index.
     *
     * @param nodeIndex Zero-based index (used to build node name).
     * @return Started Ignite node.
     */
    protected final IgniteImpl startNode(int nodeIndex) {
        return cluster.startNode(nodeIndex);
    }

    /**
     * Starts an Ignite node with the given index.
     *
     * @param nodeIndex Zero-based index (used to build node name).
     * @param nodeBootstrapConfigTemplate Bootstrap config template to use for this node.
     * @return Started Ignite node.
     */
    protected final IgniteImpl startNode(int nodeIndex, String nodeBootstrapConfigTemplate) {
        return cluster.startNode(nodeIndex, nodeBootstrapConfigTemplate);
    }

    /**
     * Stops a node by index.
     *
     * @param nodeIndex Node index.
     */
    protected final void stopNode(int nodeIndex) {
        cluster.stopNode(nodeIndex);
    }

    /**
     * Stops a node by name.
     *
     * @param name Name of the node in the cluster.
     */
    protected void stopNode(String name) {
        cluster.stopNode(name);
    }

    /**
     * Returns nodes that are started and not stopped. This can include knocked out nodes.
     */
    protected final Stream<IgniteImpl> runningNodes() {
        return cluster.runningNodes();
    }

    /**
     * Restarts a node by index.
     *
     * @param nodeIndex Node index.
     * @return New node instance.
     */
    protected final Ignite restartNode(int nodeIndex) {
        return cluster.restartNode(nodeIndex);
    }

    /**
     * Gets a node by index.
     *
     * @param index Node index.
     * @return Node by index.
     */
    protected final IgniteImpl node(int index) {
        return cluster.node(index);
    }

    protected final List<List<Object>> executeSql(String sql, Object... args) {
        return executeSql(0, sql, args);
    }

    protected final List<List<Object>> executeSql(int nodeIndex, String sql, Object... args) {
        IgniteImpl ignite = node(nodeIndex);

        return ClusterPerClassIntegrationTest.sql(ignite, null, null, sql, args);
    }
}
