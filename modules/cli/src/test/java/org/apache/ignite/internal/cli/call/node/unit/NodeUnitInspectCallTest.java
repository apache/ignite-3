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

package org.apache.ignite.internal.cli.call.node.unit;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.ignite.internal.cli.call.unit.UnitInspectCallInput;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.rest.client.model.UnitFolder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@MicronautTest(rebuildContext = true)
@WireMockTest
class NodeUnitInspectCallTest {

    private String url;

    @Inject
    private NodeUnitInspectCall call;

    @BeforeEach
    void setUp(WireMockRuntimeInfo wmRuntimeInfo) {
        url = wmRuntimeInfo.getHttpBaseUrl();
    }

    @Test
    @DisplayName("Should return unit structure with files")
    void unitStructureWithFiles() {
        // Given
        String unitId = "test.unit";
        String version = "1.0.0";

        String json = "{"
                + "\"type\":\"folder\","
                + "\"name\":\"test.unit-1.0.0\","
                + "\"size\":300,"
                + "\"children\":["
                + "{\"type\":\"file\",\"name\":\"file1.txt\",\"size\":100},"
                + "{\"type\":\"file\",\"name\":\"file2.txt\",\"size\":200}"
                + "]"
                + "}";

        stubFor(get(urlEqualTo("/management/v1/deployment/node/units/structure/" + unitId + "/" + version))
                .willReturn(ok(json)));

        // When
        UnitInspectCallInput input = UnitInspectCallInput.builder()
                .unitId(unitId)
                .version(version)
                .url(url)
                .build();

        CallOutput<UnitFolder> output = call.execute(input);

        // Then
        assertThat(output.hasError(), is(false));
        assertThat(output.body(), is(notNullValue()));
        assertThat(output.body().getName(), is("test.unit-1.0.0"));
        assertThat(output.body().getChildren(), hasSize(2));
    }

    @Test
    @DisplayName("Should return error when unit not found")
    void unitNotFound() {
        // Given
        String unitId = "non.existing.unit";
        String version = "1.0.0";

        stubFor(get(urlEqualTo("/management/v1/deployment/node/units/structure/" + unitId + "/" + version))
                .willReturn(com.github.tomakehurst.wiremock.client.WireMock.notFound()
                        .withBody("{\"title\":\"Not Found\",\"status\":404,\"detail\":\"Unit not found\"}")));

        // When
        UnitInspectCallInput input = UnitInspectCallInput.builder()
                .unitId(unitId)
                .version(version)
                .url(url)
                .build();

        CallOutput<UnitFolder> output = call.execute(input);

        // Then
        assertThat(output.hasError(), is(true));
        assertThat(output.errorCause().getMessage(), containsString("404"));
    }

    @Test
    @DisplayName("Should return unit structure with nested folders")
    void unitStructureWithNestedFolders() {
        // Given
        String unitId = "test.unit";
        String version = "1.0.0";

        String json = "{"
                + "\"type\":\"folder\","
                + "\"name\":\"test.unit-1.0.0\","
                + "\"size\":50,"
                + "\"children\":["
                + "{"
                + "\"type\":\"folder\","
                + "\"name\":\"subfolder\","
                + "\"size\":50,"
                + "\"children\":["
                + "{\"type\":\"file\",\"name\":\"nested.txt\",\"size\":50}"
                + "]"
                + "}"
                + "]"
                + "}";

        stubFor(get(urlEqualTo("/management/v1/deployment/node/units/structure/" + unitId + "/" + version))
                .willReturn(ok(json)));

        // When
        UnitInspectCallInput input = UnitInspectCallInput.builder()
                .unitId(unitId)
                .version(version)
                .url(url)
                .build();

        CallOutput<UnitFolder> output = call.execute(input);

        // Then
        assertThat(output.hasError(), is(false));
        assertThat(output.body(), is(notNullValue()));
        assertThat(output.body().getName(), is("test.unit-1.0.0"));
        assertThat(output.body().getChildren(), hasSize(1));

        UnitFolder nested = (UnitFolder) output.body().getChildren().get(0).getActualInstance();
        assertThat(nested.getName(), is("subfolder"));
        assertThat(nested.getChildren(), hasSize(1));
    }
}
