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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.InitParameters;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.sql.Session;
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
public abstract class ItAbstractThinClientTest extends IgniteAbstractTest {
    protected static final String TABLE_NAME = "TBL1";

    protected static final String COLUMN_KEY = "key";

    protected static final String COLUMN_VAL = "val";

    private final Map<String, String> nodesBootstrapCfg = new LinkedHashMap<>();

    private final List<Ignite> startedNodes = new ArrayList<>();

    private IgniteClient client;

    /**
     * Before each.
     */
    @BeforeAll
    void beforeAll(TestInfo testInfo, @WorkDirectory Path workDir) throws InterruptedException {
        this.workDir = workDir;

        String node0Name = testNodeName(testInfo, 3344);
        String node1Name = testNodeName(testInfo, 3345);

        nodesBootstrapCfg.put(
                node0Name,
                "{\n"
                        + "  network.port: 3344,\n"
                        + "  network.nodeFinder.netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                        + "  clientConnector.port: 10800\n"
                        + "}"
        );

        nodesBootstrapCfg.put(
                node1Name,
                "{\n"
                        + "  network.port: 3345,\n"
                        + "  network.nodeFinder.netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                        + "  clientConnector.sendServerExceptionStackTraceToClient: true\n"
                        + "  clientConnector.metricsEnabled: true\n"
                        + "  clientConnector.port: 10801\n"
                        + "}"
        );

        List<CompletableFuture<Ignite>> futures = nodesBootstrapCfg.entrySet().stream()
                .map(e -> TestIgnitionManager.start(e.getKey(), e.getValue(), workDir.resolve(e.getKey())))
                .collect(toList());

        String metaStorageNode = nodesBootstrapCfg.keySet().iterator().next();

        InitParameters initParameters = InitParameters.builder()
                .destinationNodeName(metaStorageNode)
                .metaStorageNodeNames(List.of(metaStorageNode))
                .clusterName("cluster")
                .build();
        IgnitionManager.init(initParameters);

        for (CompletableFuture<Ignite> future : futures) {
            assertThat(future, willCompleteSuccessfully());

            startedNodes.add(future.join());
        }

        try (Session session = startedNodes.get(0).sql().createSession()) {
            session.execute(null, "CREATE ZONE TEST_ZONE WITH REPLICAS=1, PARTITIONS=10");
            session.execute(null, "CREATE TABLE " + TABLE_NAME + "("
                    + COLUMN_KEY + " INT PRIMARY KEY, " + COLUMN_VAL + " VARCHAR) WITH PRIMARY_ZONE='TEST_ZONE'");
        }

        client = IgniteClient.builder().addresses(getClientAddresses().toArray(new String[0])).build();

        assertTrue(IgniteTestUtils.waitForCondition(() -> client.connections().size() == 2, 3000));
    }

    /**
     * After each.
     */
    @AfterAll
    void afterAll() throws Exception {
        var closeables = new ArrayList<AutoCloseable>();

        closeables.add(client);

        nodesBootstrapCfg.keySet().stream()
                .map(name -> (AutoCloseable) () -> IgnitionManager.stop(name))
                .forEach(closeables::add);

        IgniteUtils.closeAll(closeables);
    }

    protected String getNodeAddress() {
        return getClientAddresses().get(0);
    }

    protected List<String> getClientAddresses() {
        return getClientAddresses(startedNodes);
    }

    /**
     * Gets client connector addresses for the specified nodes.
     *
     * @param nodes Nodes.
     * @return List of client addresses.
     */
    public static List<String> getClientAddresses(List<Ignite> nodes) {
        List<String> res = new ArrayList<>(nodes.size());

        for (Ignite ignite : nodes) {
            int port = ((IgniteImpl) ignite).clientAddress().port();

            res.add("127.0.0.1:" + port);
        }

        return res;
    }

    protected IgniteClient client() {
        return client;
    }

    protected Ignite server() {
        return startedNodes.get(0);
    }

    /**
     * Test class.
     */
    protected static class TestPojo {
        public TestPojo() {
            //No-op.
        }

        public TestPojo(int key) {
            this.key = key;
        }

        public int key;

        public String val;
    }
}
