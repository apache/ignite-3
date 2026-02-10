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

package org.apache.ignite.internal.cli.commands.recovery.cluster.reset;

import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_CMG_NODES_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_METASTORAGE_REPLICATION_OPTION;

import org.apache.ignite.internal.cli.commands.IgniteCliInterfaceTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link ResetClusterCommand}. */
public class ResetClusterCommandTest extends IgniteCliInterfaceTestBase {
    @Override
    protected Class<?> getCommandClass() {
        return ResetClusterCommand.class;
    }

    @Test
    @DisplayName("Reset cluster with CMG nodes specified")
    void resetWithCmgNodesSpecified() {
        String nodeNames = "node1,node2";

        String expectedSentContent = "{\"cmgNodeNames\": [\"node1\", \"node2\"]}";

        returnOkForPostWithJson("/management/v1/recovery/cluster/reset", expectedSentContent);

        execute(CLUSTER_URL_OPTION, mockUrl,
                RECOVERY_CMG_NODES_OPTION, nodeNames
        );

        assertErrOutputIsEmpty();
        assertOutputIs("Successfully initiated cluster repair.");
    }

    @Test
    @DisplayName("Reset cluster with replication factor specified")
    void resetWithReplicationFactorSpecified() {
        String replicationFactor = "5";

        String expectedSentContent = "{\"metastorageReplicationFactor\" : 5}";

        returnOkForPostWithJson("/management/v1/recovery/cluster/reset", expectedSentContent);

        execute(CLUSTER_URL_OPTION, mockUrl,
                RECOVERY_METASTORAGE_REPLICATION_OPTION, replicationFactor
        );

        assertErrOutputIsEmpty();
        assertOutputIs("Successfully initiated cluster repair.");
    }

    @Test
    @DisplayName("Reset cluster with cmg node names and replication factor specified")
    void resetWithCmgNodeNamesAndReplicationFactorSpecified() {
        String replicationFactor = "5";
        String nodeNames = "node1,node2";

        String expectedSentContent = "{"
                + "    \"cmgNodeNames\": [\"node1\", \"node2\"],"
                + "    \"metastorageReplicationFactor\" : 5"
                + "}";

        returnOkForPostWithJson("/management/v1/recovery/cluster/reset", expectedSentContent);

        execute(CLUSTER_URL_OPTION, mockUrl,
                RECOVERY_CMG_NODES_OPTION, nodeNames,
                RECOVERY_METASTORAGE_REPLICATION_OPTION, replicationFactor
        );

        assertErrOutputIsEmpty();
        assertOutputIs("Successfully initiated cluster repair.");
    }

    @Test
    @DisplayName("Reset cluster fails with args not specified")
    void resetFailsArgsNotSpecified() {
        execute(CLUSTER_URL_OPTION, mockUrl);

        assertErrOutputContains("Missing required argument(s): ");
    }
}
