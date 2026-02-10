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

package org.apache.ignite.internal.cli.commands.recovery.partitions.restart;

import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_NODE_NAMES_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_PARTITION_IDS_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_WITH_CLEANUP_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_ZONE_NAME_OPTION;

import org.apache.ignite.internal.cli.commands.IgniteCliInterfaceTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link RestartPartitionsCommand}. */
public class RestartPartitionsTest extends IgniteCliInterfaceTestBase {
    private static final String PARTITIONS_RESTART_ENDPOINT = "/management/v1/recovery/zone/partitions/restart";
    private static final String PARTITIONS_RESTART_ENDPOINT_WITH_CLEANUP = "/management/v1/recovery/zone/partitions/restartWithCleanup";

    @Override
    protected Class<?> getCommandClass() {
        return RestartPartitionsCommand.class;
    }

    @Test
    @DisplayName("Restart all partitions")
    void restartAllPartitions() {
        String expectedSentContent = "{\"zoneName\" : \"zone_NAME\"}";

        returnOkForPostWithJson(PARTITIONS_RESTART_ENDPOINT, expectedSentContent, true);

        execute(CLUSTER_URL_OPTION, mockUrl,
                RECOVERY_ZONE_NAME_OPTION, "zone_NAME"
        );

        assertErrOutputIsEmpty();
        assertOutputIs("Successfully restarted partitions.");
    }

    @Test
    @DisplayName("Restart specified partitions")
    void restartSpecifiedPartitions() {
        String expectedSentContent = "{\"partitionIds\" : [1,2]}";

        returnOkForPostWithJson(PARTITIONS_RESTART_ENDPOINT, expectedSentContent, true);

        execute(CLUSTER_URL_OPTION, mockUrl,
                RECOVERY_ZONE_NAME_OPTION, "zone_NAME",
                RECOVERY_PARTITION_IDS_OPTION, "1,2"
        );

        assertErrOutputIsEmpty();
        assertOutputIs("Successfully restarted partitions.");
    }

    @Test
    @DisplayName("Restart specified nodes")
    void restartSpecifiedNodes() {
        String expectedSentContent = "{\"nodeNames\" : [\"node_NAME\",\"node_NAME_2\"]}";

        returnOkForPostWithJson(PARTITIONS_RESTART_ENDPOINT, expectedSentContent, true);

        execute(CLUSTER_URL_OPTION, mockUrl,
                RECOVERY_ZONE_NAME_OPTION, "zone_NAME",
                RECOVERY_NODE_NAMES_OPTION, "node_NAME,node_NAME_2"
        );

        assertErrOutputIsEmpty();
        assertOutputIs("Successfully restarted partitions.");
    }

    @Test
    @DisplayName("Restart all partitions with cleanup")
    void restartAllPartitionsWithCleanup() {
        String expectedSentContent = "{\"zoneName\" : \"zone_NAME\"}";

        returnOkForPostWithJson(PARTITIONS_RESTART_ENDPOINT_WITH_CLEANUP, expectedSentContent, true);

        execute(CLUSTER_URL_OPTION, mockUrl,
                RECOVERY_ZONE_NAME_OPTION, "zone_NAME",
                RECOVERY_WITH_CLEANUP_OPTION
        );

        assertErrOutputIsEmpty();
        assertOutputIs("Successfully restarted partitions.");
    }

    @Test
    @DisplayName("Restart specified partitions with cleanup")
    void restartSpecifiedPartitionsWithCleanup() {
        String expectedSentContent = "{\"partitionIds\" : [1,2]}";

        returnOkForPostWithJson(PARTITIONS_RESTART_ENDPOINT_WITH_CLEANUP, expectedSentContent, true);

        execute(CLUSTER_URL_OPTION, mockUrl,
                RECOVERY_ZONE_NAME_OPTION, "zone_NAME",
                RECOVERY_PARTITION_IDS_OPTION, "1,2",
                RECOVERY_WITH_CLEANUP_OPTION
        );

        assertErrOutputIsEmpty();
        assertOutputIs("Successfully restarted partitions.");
    }
}
