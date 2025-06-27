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

package org.apache.ignite.internal.eventlog;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration;
import org.apache.ignite.internal.eventlog.config.schema.EventLogExtensionConfiguration;
import org.apache.ignite.internal.eventlog.config.schema.WebhookSinkChange;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ItWebhookSinkConfigurationValidationTest extends ClusterPerClassIntegrationTest {
    @ParameterizedTest
    @ValueSource(strings = {"http/json", "HTTP/JSON"})
    void validProtocolTest(String protocol) {
        assertThat(
                eventLogConfiguration().change(c ->
                        c.changeSinks().create("webhookSink" + UUID.randomUUID(), s -> {
                            s.convert(WebhookSinkChange.class)
                                    .changeEndpoint("http://localhost")
                                    .changeProtocol(protocol);
                        })),
                willCompleteSuccessfully()
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"INVALID", "123", "null", "http/protobuf"})
    void invalidProtocolTest(String protocol) {
        assertThrows(
                Exception.class,
                () -> eventLogConfiguration().change(c ->
                        c.changeSinks().create("webhookSink" + UUID.randomUUID(), s -> {
                            s.convert(WebhookSinkChange.class)
                                    .changeEndpoint("http://localhost")
                                    .changeProtocol(protocol);
                        })).join()
        );
    }

    private static IgniteImpl aliveIgniteImpl() {
        return unwrapIgniteImpl(CLUSTER.aliveNode());
    }

    private static EventLogConfiguration eventLogConfiguration() {
        return aliveIgniteImpl().clusterConfiguration().getConfiguration(EventLogExtensionConfiguration.KEY).eventlog();
    }
}

