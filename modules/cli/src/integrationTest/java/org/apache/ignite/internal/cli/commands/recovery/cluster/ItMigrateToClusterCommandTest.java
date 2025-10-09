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

package org.apache.ignite.internal.cli.commands.recovery.cluster;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_CMG_NODES_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_NEW_CLUSTER_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_OLD_CLUSTER_URL_OPTION;

import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.cli.commands.recovery.cluster.migrate.MigrateToClusterCommand;
import org.apache.ignite.network.NodeMetadata;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/** Test class for {@link MigrateToClusterCommand}. */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ItMigrateToClusterCommandTest extends ItSystemDisasterRecoveryCliTest {
    @Override
    protected int initialNodes() {
        return 2;
    }

    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        builder.cmgNodeNames(CLUSTER.nodeName(0));
        builder.metaStorageNodeNames(CLUSTER.nodeName(1));
    }

    @Test
    @Order(Integer.MAX_VALUE) // This test is run last as it restarts the node.
    void initiatesNodesMigration() throws Exception {
        String restUrl0 = restUrl(0);
        String restUrl1 = restUrl(1);

        // Stop the CMG majority.
        CLUSTER.stopNode(0);

        // First just initiate repair (we need it to have ability to trigger a migration).
        repairClusterOnNode1(restUrl1);

        CLUSTER.startEmbeddedNode(0);

        // Now actually do migration.
        execute(
                "recovery", "cluster", "migrate",
                RECOVERY_OLD_CLUSTER_URL_OPTION, restUrl0,
                RECOVERY_NEW_CLUSTER_URL_OPTION, restUrl1
        );

        try {
            assertErrOutputIsEmpty();
            assertOutputContains("Node has gone, this most probably means that migration is initiated and the node restarts.");
        } finally {
            waitTillNodeRestartsInternally(0);
        }
    }

    private void repairClusterOnNode1(String restUrl1) throws InterruptedException {
        execute(
                "recovery", "cluster", "reset",
                CLUSTER_URL_OPTION, restUrl1,
                RECOVERY_CMG_NODES_OPTION, CLUSTER.nodeName(1)
        );

        try {
            assertErrOutputIsEmpty();
            assertOutputContains("Node has gone, this most probably means that cluster repair is initiated and the node restarts.");
        } finally {
            waitTillNodeRestartsInternally(1);
        }
    }

    private static String restUrl(int nodeIndex) {
        NodeMetadata nodeMetadata = unwrapIgniteImpl(CLUSTER.node(nodeIndex)).node().nodeMetadata();

        return "http://" + nodeMetadata.restHost() + ":" + nodeMetadata.httpPort();
    }
}
