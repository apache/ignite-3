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

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesRegex;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.client.BasicAuthenticator;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ItEventLogTest extends ClusterPerClassIntegrationTest {
    private static final String PROVIDER_NAME = "basic";

    private static final String USERNAME = "admin";

    private static final String PASSWORD = "password";

    private static Path eventlogPath;

    @BeforeAll
    static void captureEventLogPath() {
        String buildDirPath = System.getProperty("buildDirPath");
        eventlogPath = Path.of(buildDirPath).resolve("event.log");
    }

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Override
    protected void configureInitParameters(InitParametersBuilder builder) {
        String securityConfiguration = "security {\n"
                + "  enabled:true,\n"
                + "  authentication.providers." + PROVIDER_NAME + ":{\n"
                + "    type:basic, users." + USERNAME + ".password:" + PASSWORD + "\n"
                + "  }\n},\n";

        String eventLog = "eventlog {\n"
                + " sinks.logSink.channel: testChannel,\n"
                + " channels.testChannel.events: [USER_AUTHENTICATED],\n"
                + "}\n";

        builder.clusterConfiguration(securityConfiguration + eventLog);
    }

    @Test
    void logsToFile() throws Exception {
        // Given the event log is initially empty.
        assertThat(readEventLog(), is(empty()));

        // When client is connected.
        BasicAuthenticator authenticator = BasicAuthenticator.builder().username(USERNAME).password(PASSWORD).build();
        try (IgniteClient ignored = IgniteClient.builder().addresses("127.0.0.1:10800").authenticator(authenticator).build()) {
            // do nothing
        }

        // Then single event is written to file.
        await().until(ItEventLogTest::readEventLog, hasSize(1));

        // And event is written in JSON format.
        String expectedEventJsonPattern = "\\{"
                + "\"type\":\"USER_AUTHENTICATED\","
                + "\"timestamp\":\\d*,"
                + "\"productVersion\":\".*\","
                + "\"user\":\\{\"username\":\"" + USERNAME + "\",\"authenticationProvider\":\"" + PROVIDER_NAME + "\"},"
                + "\"fields\":\\{}"
                + "}";

        assertThat(readEventLog(), contains(matchesRegex(expectedEventJsonPattern)));
    }

    private static List<String> readEventLog() throws IOException {
        return Files.readAllLines(eventlogPath);
    }
}
