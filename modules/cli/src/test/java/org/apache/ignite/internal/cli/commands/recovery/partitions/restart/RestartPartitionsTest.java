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

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_URL_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_NODE_NAMES_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_PARTITION_IDS_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_TABLE_NAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_WITH_CLEANUP_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.RECOVERY_ZONE_NAME_OPTION;
import static org.apache.ignite.internal.rest.constants.MediaType.APPLICATION_JSON_UTF8;

import org.apache.ignite.internal.cli.commands.IgniteCliInterfaceTestBase;
import org.apache.ignite.internal.util.ArrayUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link RestartPartitionsCommand}. */
public class RestartPartitionsTest extends IgniteCliInterfaceTestBase {
    private static String PARTITIONS_RESTART_ENDPOINT;
    private static String PARTITIONS_RESTART_ENDPOINT_WITH_CLEANUP;

    @BeforeAll
    public static void beforeAll() {
        PARTITIONS_RESTART_ENDPOINT = "partitions/restart";
        PARTITIONS_RESTART_ENDPOINT_WITH_CLEANUP = "partitions/restartWithCleanup";
    }

    @Test
    @DisplayName("Restart all partitions")
    void restartAllPartitions() {
        String expectedSentContent;

        expectedSentContent = "{"
                + "     \"zoneName\" : \"zone_NAME\""
                + "}";

        stubFor(post("/management/v1/recovery/" + PARTITIONS_RESTART_ENDPOINT)
                .withRequestBody(equalToJson(expectedSentContent, true, true))
                .withHeader("Content-Type", equalTo(APPLICATION_JSON_UTF8))
                .willReturn(ok()));

        execute(CLUSTER_URL_OPTION, mockUrl,
                RECOVERY_TABLE_NAME_OPTION, "table_NAME",
                RECOVERY_ZONE_NAME_OPTION, "zone_NAME"
        );

        assertErrOutputIsEmpty();
        assertOutputIs("Successfully restarted partitions.");
    }

    @Test
    @DisplayName("Restart specified partitions")
    void restartSpecifiedPartitions() {
        String expectedSentContent = "{\"partitionIds\" : [1,2]}";

        stubFor(post("/management/v1/recovery/" + PARTITIONS_RESTART_ENDPOINT)
                .withRequestBody(equalToJson(expectedSentContent, true, true))
                .withHeader("Content-Type", equalTo(APPLICATION_JSON_UTF8))
                .willReturn(ok()));

        execute(CLUSTER_URL_OPTION, mockUrl,
                RECOVERY_TABLE_NAME_OPTION, "table_NAME",
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

        stubFor(post("/management/v1/recovery/" + PARTITIONS_RESTART_ENDPOINT)
                .withRequestBody(equalToJson(expectedSentContent, true, true))
                .withHeader("Content-Type", equalTo(APPLICATION_JSON_UTF8))
                .willReturn(ok()));

        execute(CLUSTER_URL_OPTION, mockUrl,
                RECOVERY_TABLE_NAME_OPTION, "table_NAME",
                RECOVERY_ZONE_NAME_OPTION, "zone_NAME",
                RECOVERY_NODE_NAMES_OPTION, "node_NAME,node_NAME_2"
        );

        assertErrOutputIsEmpty();
        assertOutputIs("Successfully restarted partitions.");
    }

    @Test
    @DisplayName("Restart all partitions with cleanup")
    void restartAllPartitionsWithCleanup() {
        String expectedSentContent;

        expectedSentContent = "{"
                + "     \"zoneName\" : \"zone_NAME\""
                + "}";

        stubFor(post("/management/v1/recovery/" + PARTITIONS_RESTART_ENDPOINT_WITH_CLEANUP)
                .withRequestBody(equalToJson(expectedSentContent, true, true))
                .withHeader("Content-Type", equalTo(APPLICATION_JSON_UTF8))
                .willReturn(ok()));

        execute(CLUSTER_URL_OPTION, mockUrl,
                RECOVERY_TABLE_NAME_OPTION, "table_NAME",
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

        stubFor(post("/management/v1/recovery/" + PARTITIONS_RESTART_ENDPOINT_WITH_CLEANUP)
                .withRequestBody(equalToJson(expectedSentContent, true, true))
                .withHeader("Content-Type", equalTo(APPLICATION_JSON_UTF8))
                .willReturn(ok()));

        execute(CLUSTER_URL_OPTION, mockUrl,
                RECOVERY_TABLE_NAME_OPTION, "table_NAME",
                RECOVERY_ZONE_NAME_OPTION, "zone_NAME",
                RECOVERY_PARTITION_IDS_OPTION, "1,2",
                RECOVERY_WITH_CLEANUP_OPTION
        );

        assertErrOutputIsEmpty();
        assertOutputIs("Successfully restarted partitions.");
    }

    @Override
    protected void execute(String... args) {
        String[] fullArgs = ArrayUtils.concat(new String[] {"recovery", "partitions", "restart"}, args);

        super.execute(fullArgs);
    }
}
