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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.sql.engine.util.QueryChecker.matches;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests to make sure sql engine can recover execution when run on unstable topology.
 */
public class ItUnstableTopologyTest extends BaseSqlIntegrationTest {
    private static final String DATA_NODE_BOOTSTRAP_CFG_TEMPLATE = "ignite {\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder: {\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    }\n"
            + "  },\n"
            + "  clientConnector: { port:{} },\n"
            + "  nodeAttributes: {\n"
            + "    nodeAttributes: { role: data }\n"
            + "  },\n"
            + "  rest.port: {},\n"
            + "  failureHandler.dumpThreadsOnFailure: false\n"
            + "}";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @AfterEach
    public void dropTables() {
        dropAllTables();
        dropAllZonesExceptDefaultOne();
    }

    @Test
    public void ensureLostOfNodeDoesntCausesQueryToFail() {
        CLUSTER.startNode(1, DATA_NODE_BOOTSTRAP_CFG_TEMPLATE);
        CLUSTER.startNode(2, DATA_NODE_BOOTSTRAP_CFG_TEMPLATE);
        CLUSTER.startNode(3, DATA_NODE_BOOTSTRAP_CFG_TEMPLATE);

        sql("CREATE ZONE my_zone ("
                + "partitions 1, "
                + "replicas 3, "
                + "nodes filter '$[?(@.role == \"data\")]') "
                + "storage profiles ['default']");

        sql("CREATE TABLE my_table (id INT PRIMARY KEY, val INT) ZONE my_zone STORAGE PROFILE 'default'");
        assertQuery("INSERT INTO my_table SELECT x, x FROM system_range(1, 1000)")
                .returns(1000L)
                .check();

        Ignite gateway = CLUSTER.node(0); 

        assertQuery(gateway, "SELECT count(*) FROM my_table WHERE val > -1")
                .matches(matches(".*TableScan.*"))
                .returns(1000L)
                .check();

        // The choice of node to stop depends on current mapping algorithm which sorts
        // nodes by name and chooses the first one among all options, therefore we stop
        // 1th node -- the first data node. This invariant is fragile, but without
        // EXPLAIN MAPPING FOR <query> it seems there is no other options to derive
        // information about execution nodes for particular query.
        CLUSTER.stopNode(1);

        assertQuery(gateway, "SELECT count(*) FROM my_table WHERE val > -1")
                .matches(matches(".*TableScan.*"))
                .returns(1000L)
                .check();
    }
}
