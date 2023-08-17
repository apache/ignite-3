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

import java.nio.file.Path;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.QueryContext;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.property.PropertiesHelper;
import org.apache.ignite.internal.sql.engine.session.SessionId;
import org.apache.ignite.internal.sql.engine.util.TestQueryProcessor;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

/**
 * Abstract integration test that starts and stops a cluster per test method.
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
            + "  },\n"
            + "  clientConnector: { port:{} }\n"
            + "}";

    /** Template for node bootstrap config with Scalecube settings for fast failure detection. */
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
            + "  },\n"
            + "  clientConnector: { port:{} }\n"
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
            + "  clientConnector: { port:{} }\n"
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

        cluster = new Cluster(testInfo, workDir, getNodeBootstrapConfigTemplate());

        if (initialNodes() > 0) {
            cluster.startAndInit(initialNodes(), cmgMetastoreNodes());
        }
    }

    /**
     * Invoked after each test has finished.
     *
     * @param testInfo Test information oject.
     * @throws Exception If failed.
     */
    @AfterEach
    @Timeout(60)
    public void tearDown(TestInfo testInfo) throws Exception {
        tearDownBase(testInfo);

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
        QueryProcessor qryProc = new TestQueryProcessor(node(0));
        SessionId sessionId = qryProc.createSession(PropertiesHelper.emptyHolder());
        QueryContext context = QueryContext.create(SqlQueryType.ALL, node(0).transactions());

        return getAllFromCursor(
                qryProc.querySingleAsync(sessionId, context, sql, args).join()
        );
    }
}
