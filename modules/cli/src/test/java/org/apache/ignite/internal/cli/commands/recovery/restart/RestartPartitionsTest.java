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

package org.apache.ignite.internal.cli.commands.recovery.restart;

import static org.mockserver.matchers.MatchType.ONLY_MATCHING_FIELDS;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.JsonBody.json;

import org.apache.ignite.internal.cli.commands.IgniteCliInterfaceTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockserver.model.MediaType;

/** Unit tests for {@link RestartPartitionsCommand}. */
public class RestartPartitionsTest extends IgniteCliInterfaceTestBase {
    @Test
    @DisplayName("Restart all partitions")
    void restartAllPartitions() {
        String expectedSentContent = "{"
                + "     \"tableName\" : \"table_NAME\","
                + "     \"zoneName\" : \"zone_NAME\","
                + "}";

        clientAndServer
                .when(request()
                        .withMethod("POST")
                        .withPath("/management/v1/recovery/restart-partitions")
                        .withBody(json(expectedSentContent))
                        .withContentType(MediaType.APPLICATION_JSON_UTF_8)
                )
                .respond(response(null));

        execute("recovery restart-partitions --cluster-endpoint-url " + mockUrl
                + " --table table_NAME --zone zone_NAME");

        assertSuccessfulOutputIs("Successfully restarted partitions without cleanup.");
    }

    @Test
    @DisplayName("Restart specified partitions")
    void restartSpecifiedPartitions() {
        String expectedSentContent = "{\"partitionIds\" : [1,2]}";

        clientAndServer
                .when(request()
                        .withMethod("POST")
                        .withPath("/management/v1/recovery/restart-partitions")
                        .withBody(json(expectedSentContent, ONLY_MATCHING_FIELDS))
                        .withContentType(MediaType.APPLICATION_JSON_UTF_8)
                )
                .respond(response(null));

        execute("recovery restart-partitions --cluster-endpoint-url " + mockUrl
                + " --table table_NAME --zone zone_NAME --partitions 1,2");

        assertSuccessfulOutputIs("Successfully restarted partitions without cleanup.");
    }

    @Test
    @DisplayName("Restart specified nodes")
    void restartSpecifiedNodes() {
        String expectedSentContent = "{\"nodeNames\" : [\"node_NAME\",\"node_NAME_2\"]}";

        clientAndServer
                .when(request()
                        .withMethod("POST")
                        .withPath("/management/v1/recovery/restart-partitions")
                        .withBody(json(expectedSentContent, ONLY_MATCHING_FIELDS))
                        .withContentType(MediaType.APPLICATION_JSON_UTF_8)
                )
                .respond(response(null));

        execute("recovery restart-partitions --cluster-endpoint-url " + mockUrl
                + " --table table_NAME --zone zone_NAME --nodes node_NAME,node_NAME_2");

        assertSuccessfulOutputIs("Successfully restarted partitions without cleanup.");
    }

    @Test
    @DisplayName("Restart with purge")
    void restartPartitionsPurge() {
        String expectedSentContent = "{\"purge\" : true}";

        clientAndServer
                .when(request()
                        .withMethod("POST")
                        .withPath("/management/v1/recovery/restart-partitions")
                        .withBody(json(expectedSentContent, ONLY_MATCHING_FIELDS))
                        .withContentType(MediaType.APPLICATION_JSON_UTF_8)
                )
                .respond(response(null));

        execute("recovery restart-partitions --cluster-endpoint-url " + mockUrl
                + " --table table_NAME --zone zone_NAME --purge");

        assertSuccessfulOutputIs("Successfully restarted partitions with cleanup.");
    }
}
