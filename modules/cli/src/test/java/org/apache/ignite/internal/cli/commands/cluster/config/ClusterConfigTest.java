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

package org.apache.ignite.internal.cli.commands.cluster.config;

import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import org.apache.ignite.internal.cli.commands.IgniteCliInterfaceTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests "cluster config" commands. */
@DisplayName("cluster config")
class ClusterConfigTest extends IgniteCliInterfaceTestBase {
    @Test
    @DisplayName("show --cluster-endpoint-url http://localhost:10300")
    void show() {
        clientAndServer
                .when(request()
                        .withMethod("GET")
                        .withPath("/management/v1/configuration/cluster")
                )
                .respond(response("{\"autoAdjust\":{\"enabled\":true}}"));

        execute("cluster config show --cluster-endpoint-url " + mockUrl);

        assertSuccessfulOutputIs("{\n"
                + "  \"autoAdjust\" : {\n"
                + "    \"enabled\" : true\n"
                + "  }\n"
                + "}\n");
    }

    @Test
    @DisplayName("show --cluster-endpoint-url http://localhost:10300 local.baseline")
    void showSubtree() {
        clientAndServer
                .when(request()
                        .withMethod("GET")
                        .withPath("/management/v1/configuration/cluster/local.baseline")
                )
                .respond(response("{\"autoAdjust\":{\"enabled\":true}}"));

        execute("cluster config show --cluster-endpoint-url " + mockUrl + " local.baseline");

        assertSuccessfulOutputIs("{\n"
                + "  \"autoAdjust\" : {\n"
                + "    \"enabled\" : true\n"
                + "  }\n"
                + "}\n");
    }

    @Test
    @DisplayName("update --cluster-endpoint-url http://localhost:10300 local.baseline.autoAdjust.enabled=true")
    void updateHocon() {
        clientAndServer
                .when(request()
                        .withMethod("PATCH")
                        .withPath("/management/v1/configuration/cluster")
                        .withBody("local.baseline.autoAdjust.enabled=true")
                )
                .respond(response(null));

        execute("cluster config update --cluster-endpoint-url "
                + mockUrl + " local.baseline.autoAdjust.enabled=true");

        assertSuccessfulOutputIs("Cluster configuration was updated successfully");
    }
}
