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

package org.apache.ignite.internal.cli.commands.node.config;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.patch;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.apache.ignite.internal.rest.constants.MediaType.TEXT_PLAIN;

import org.apache.ignite.internal.cli.commands.IgniteCliInterfaceTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests "node config" commands. */
@DisplayName("node config")
public class NodeConfigTest extends IgniteCliInterfaceTestBase {
    @Test
    @DisplayName("show --url http://localhost:10300")
    void show() {
        stubFor(get("/management/v1/configuration/node")
                .willReturn(ok("{\"autoAdjust\":{\"enabled\":true}}").withHeader("Content-Type", TEXT_PLAIN)));

        execute("node config show --url " + mockUrl);

        assertSuccessfulOutputIs("autoAdjust {\n"
                + "    enabled=true\n"
                + "}\n");
    }

    @Test
    @DisplayName("show --url http://localhost:10300/")
    void trailingSlash() {
        stubFor(get("/management/v1/configuration/node")
                .willReturn(ok("{\"autoAdjust\":{\"enabled\":true}}").withHeader("Content-Type", TEXT_PLAIN)));

        execute("node config show --url " + mockUrl + "/");

        assertSuccessfulOutputIs("autoAdjust {\n"
                + "    enabled=true\n"
                + "}\n");
    }

    @Test
    @DisplayName("show --url http://localhost:10300 local.baseline")
    void showSubtree() {
        stubFor(get("/management/v1/configuration/node/local.baseline")
                .willReturn(ok("{\"autoAdjust\":{\"enabled\":true}}").withHeader("Content-Type", TEXT_PLAIN)));

        execute("node config show --url " + mockUrl + " local.baseline");

        assertSuccessfulOutputIs("autoAdjust {\n"
                + "    enabled=true\n"
                + "}\n");
    }

    @Test
    @DisplayName("update --url http://localhost:10300 local.baseline.autoAdjust.enabled=true")
    void updateHocon() {
        stubFor(patch("/management/v1/configuration/node")
                .withRequestBody(equalTo("local.baseline.autoAdjust.enabled=true"))
                .willReturn(ok()));

        execute("node config update --url " + mockUrl + " local.baseline.autoAdjust.enabled=true");

        assertSuccessfulOutputIs("Node configuration updated. Restart the node to apply changes.");
    }
}
