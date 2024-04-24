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

package org.apache.ignite.internal.eventlog.impl;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.eventlog.api.Event;
import org.apache.ignite.internal.eventlog.api.IgniteEvents;
import org.apache.ignite.internal.eventlog.api.Sink;
import org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration;
import org.apache.ignite.internal.eventlog.config.schema.LogSinkChange;
import org.apache.ignite.internal.eventlog.event.EventUser;
import org.apache.ignite.internal.eventlog.ser.EventSerializerFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
class LogSinkTest extends BaseIgniteAbstractTest {

    @InjectConfiguration
    private EventLogConfiguration cfg;

    private static Path eventlogPath;

    @BeforeAll
    static void beforeAll() {
        String buildDirPath = System.getProperty("buildDirPath");
        eventlogPath = Path.of(buildDirPath).resolve("event.log");

    }

    @AfterAll
    static void afterAll() throws IOException {
        Files.deleteIfExists(eventlogPath);
    }

    @Test
    void logsToFile() throws Exception {
        // Given log sink configuration.
        cfg.change(c -> c.changeSinks().create("logSink", s -> {
            LogSinkChange logSinkChange = (LogSinkChange) s.convert("log");
            logSinkChange.changeCriteria("EventLog");
            logSinkChange.changeLevel("INFO");
            logSinkChange.changeFormat("json");
        })).get();
        // And log sink.
        Sink logSink = new LogSinkFactory(new EventSerializerFactory().createEventSerializer())
                .createSink(cfg.sinks().get("logSink").value());
        // And event.
        Event event = IgniteEvents.USER_AUTHENTICATED.create(
                EventUser.of("user1", "basicProvider")
        );

        // When write event into log sink.
        logSink.write(event);

        // Then event is written to file.
        await().untilAsserted(() -> assertThat(Files.readAllLines(eventlogPath), hasSize(1)));
        // And event is written in JSON format.
        var expectedEventJson = "{"
                + "\"type\":\"USER_AUTHENTICATED\","
                + "\"timestamp\":" + event.getTimestamp() + ","
                + "\"productVersion\":\"" + event.getProductVersion() + "\","
                + "\"user\":{\"username\":\"user1\",\"authenticationProvider\":\"basicProvider\"},"
                + "\"fields\":{}"
                + "}";
        assertThat(Files.readAllLines(eventlogPath), hasItem(expectedEventJson));

        // When write one more event.
        Event event2 = IgniteEvents.CONNECTION_CLOSED.create(
                EventUser.of("user2", "basicProvider")
        );

        logSink.write(event2);

        // Then both events are written to file.
        await().untilAsserted(() -> assertThat(Files.readAllLines(eventlogPath), hasSize(2)));
    }
}
