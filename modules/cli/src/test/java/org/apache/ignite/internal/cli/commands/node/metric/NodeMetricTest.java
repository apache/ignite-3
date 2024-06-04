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

package org.apache.ignite.internal.cli.commands.node.metric;

import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import org.apache.ignite.internal.cli.commands.IgniteCliInterfaceTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests "node metric" commands. */
@DisplayName("node metric")
class NodeMetricTest extends IgniteCliInterfaceTestBase {
    @Test
    @DisplayName("source enable srcName")
    void enable() {
        clientAndServer
                .when(request()
                        .withMethod("POST")
                        .withPath("/management/v1/metric/node/enable")
                        .withBody("srcName")
                )
                .respond(response(null));

        execute("node metric source enable --url " + mockUrl + " srcName");

        assertSuccessfulOutputIs("Metric source was enabled successfully");
    }

    @Test
    @DisplayName("source disable srcName")
    void disable() {
        clientAndServer
                .when(request()
                        .withMethod("POST")
                        .withPath("/management/v1/metric/node/disable")
                        .withBody("srcName")
                )
                .respond(response(null));

        execute("node metric source disable --url " + mockUrl + " srcName");

        assertSuccessfulOutputIs("Metric source was disabled successfully");
    }

    @Test
    @DisplayName("source list")
    void listSources() {
        String responseBody = "[{\"name\":\"enabledMetric\",\"enabled\":true},{\"name\":\"disabledMetric\",\"enabled\":false}]";
        clientAndServer
                .when(request()
                        .withMethod("GET")
                        .withPath("/management/v1/metric/node/source")
                )
                .respond(response(responseBody));

        execute("node metric source list --plain --url " + mockUrl);

        assertSuccessfulOutputIs("Set name\tEnabled\nenabledMetric\tenabled\ndisabledMetric\tdisabled\n");
    }

    @Test
    @DisplayName("list")
    void listSets() {
        String responseBody = "[{\"name\":\"metricSet\",\"metrics\":[{\"name\":\"metric\",\"desc\":\"description\"}]}]";
        clientAndServer
                .when(request()
                        .withMethod("GET")
                        .withPath("/management/v1/metric/node/set")
                )
                .respond(response(responseBody));

        execute("node metric list --plain --url " + mockUrl);

        assertSuccessfulOutputIs("Set name\tMetric name\tDescription\nmetricSet\t\t\n\tmetric\tdescription");
    }
}
