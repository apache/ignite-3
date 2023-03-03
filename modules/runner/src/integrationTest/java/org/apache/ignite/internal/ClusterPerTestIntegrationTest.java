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

import static org.apache.ignite.internal.sql.engine.util.CursorUtils.getAllFromCursor;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.platform.commons.support.ReflectionSupport;

/**
 * Abstract integration test that starts and stops a cluster test method.
 */
@SuppressWarnings("ALL")
public abstract class ClusterPerTestIntegrationTest extends IgniteIntegrationTest {
    private static final IgniteLogger LOG = Loggers.forClass(ClusterPerTestIntegrationTest.class);

    /** Base port number. */
    private static final int BASE_PORT = 3344;

    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG_TEMPLATE = "{\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder: {\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    }\n"
            + "  }\n"
            + "}";

    /** Template for node bootstrap config with Scalecube and Logical Topology settings for fast failure detection. */
    protected static final String FAST_FAILURE_DETECTION_NODE_BOOTSTRAP_CFG_TEMPLATE = "{\n"
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
            + "  },"
            + "  cluster.failoverTimeout: 100\n"
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
        setupBase(testInfo, workDir);
    }

    /**
     * Invoked after each test has finished.
     *
     * @param testInfo Test information oject.
     * @throws Exception If failed.
     */
    @AfterEach
    public void tearDown(TestInfo testInfo) throws Exception {
        tearDownBase(testInfo);
    }

    @BeforeEach
    void startAndInitCluster(TestInfo testInfo) {
        cluster = new Cluster(testInfo, workDir, getNodeBootstrapConfigTemplate());

        cluster.startAndInit(initialNodes());
    }

    private String invokeArglessMethod(Class<?> testClass, String methodName) {
        Method method = ReflectionSupport.findMethod(testClass, methodName).orElseThrow();

        if (!Modifier.isStatic(method.getModifiers())) {
            throw new IllegalStateException(methodName + " is expected to be static");
        }

        return (String) ReflectionSupport.invokeMethod(method, null);
    }

    @AfterEach
    @Timeout(60)
    void shutdownCluster() {
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
        return getAllFromCursor(
                node(0).queryEngine().queryAsync("PUBLIC", sql, args).get(0).join()
        );
    }
}
