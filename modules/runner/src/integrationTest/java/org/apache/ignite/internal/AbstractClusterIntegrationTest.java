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

package org.apache.ignite.internal;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Abstract integration test that starts and stops a cluster.
 */
@ExtendWith(WorkDirectoryExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractClusterIntegrationTest extends BaseIgniteAbstractTest {
    private static final IgniteLogger LOG = IgniteLogger.forClass(AbstractClusterIntegrationTest.class);

    /** Base port number. */
    private static final int BASE_PORT = 3344;

    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG = "{\n"
            + "  \"node\": {\n"
            + "    \"metastorageNodes\":[ {} ]\n"
            + "  },\n"
            + "  \"network\": {\n"
            + "    \"port\":{},\n"
            + "    \"nodeFinder\":{\n"
            + "      \"netClusterNodes\": [ {} ]\n"
            + "    }\n"
            + "  }\n"
            + "}";

    /** Cluster nodes. */
    protected final List<Ignite> clusterNodes = new ArrayList<>();

    /** Work directory. */
    @WorkDirectory
    private static Path WORK_DIR;

    /**
     * Before all.
     *
     * @param testInfo Test information oject.
     */
    @BeforeEach
    void startNodes(TestInfo testInfo) {
        //TODO: IGNITE-16034 Here we assume that Metastore consists of one node, and it starts first.
        String metastorageNodes = '\"' + IgniteTestUtils.testNodeName(testInfo, 0) + '\"';

        String connectNodeAddr = "\"localhost:" + BASE_PORT + '\"';

        for (int i = 0; i < nodes(); i++) {
            String curNodeName = IgniteTestUtils.testNodeName(testInfo, i);

            clusterNodes.add(IgnitionManager.start(curNodeName, IgniteStringFormatter.format(NODE_BOOTSTRAP_CFG,
                    metastorageNodes,
                    BASE_PORT + i,
                    connectNodeAddr
            ), WORK_DIR.resolve(curNodeName)));
        }
    }

    /**
     * Get a count of nodes in the Ignite cluster.
     *
     * @return Count of nodes.
     */
    protected int nodes() {
        return 3;
    }

    /**
     * After all.
     */
    @AfterEach
    void stopNodes() throws Exception {
        LOG.info("Start tearDown()");

        IgniteUtils.closeAll(ItUtils.reverse(clusterNodes));

        clusterNodes.clear();

        LOG.info("End tearDown()");
    }

    /**
     * Invokes before the test will start.
     *
     * @param testInfo Test information oject.
     * @throws Exception If failed.
     */
    @BeforeEach
    public void setup(TestInfo testInfo) throws Exception {
        setupBase(testInfo, WORK_DIR);
    }

    /**
     * Invokes after the test has finished.
     *
     * @param testInfo Test information oject.
     * @throws Exception If failed.
     */
    @AfterEach
    public void tearDown(TestInfo testInfo) throws Exception {
        tearDownBase(testInfo);
    }

    protected final IgniteImpl node(int index) {
        return (IgniteImpl) clusterNodes.get(index);
    }
}
