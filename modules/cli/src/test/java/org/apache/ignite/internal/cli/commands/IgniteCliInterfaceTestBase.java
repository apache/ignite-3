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

package org.apache.ignite.internal.cli.commands;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.apache.ignite.internal.rest.constants.MediaType.APPLICATION_JSON_UTF8;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.BeforeAll;

/**
 * Base class for testing CLI interface.
 */
@WireMockTest
public class IgniteCliInterfaceTestBase extends CliCommandTestBase {
    protected static String mockUrl;

    protected static void returnOkForPostWithJson(String partitionsRestartEndpoint, String expectedSentContent) {
        returnOkForPostWithJson(partitionsRestartEndpoint, expectedSentContent, false);
    }

    protected static void returnOkForPostWithJson(
            String partitionsRestartEndpoint,
            String expectedSentContent,
            boolean ignoreExtraElements
    ) {
        stubFor(post(partitionsRestartEndpoint)
                .withRequestBody(equalToJson(expectedSentContent, false, ignoreExtraElements))
                .withHeader("Content-Type", equalTo(APPLICATION_JSON_UTF8))
                .willReturn(ok()));
    }

    @Override
    protected Class<?> getCommandClass() {
        return TopLevelCliCommand.class;
    }

    @BeforeAll
    static void initMockServer(WireMockRuntimeInfo wmRuntimeInfo) {
        mockUrl = wmRuntimeInfo.getHttpBaseUrl();
    }
}
