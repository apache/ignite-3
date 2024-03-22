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

package org.apache.ignite.internal.eventlog.sink;

import static org.hamcrest.MatcherAssert.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.eventlog.api.Event;
import org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration;
import org.apache.ignite.internal.eventlog.config.schema.LogSinkView;
import org.apache.ignite.internal.eventlog.event.EventUser;
import org.apache.ignite.internal.eventlog.event.IgniteEvents;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
public class LogSinkTest {

    @InjectConfiguration
    private EventLogConfiguration eventLogConfiguration;

    static File eventlogFile;

    @BeforeAll
    static void beforeAll() {
        String buildDirPath = System.getProperty("buildDirPath");
        eventlogFile = Path.of(buildDirPath).resolve("event.log").toFile();
        if (eventlogFile.exists()) {
            eventlogFile.delete();
        }
    }

    @Test
    void logsToFile() throws IOException {
        var logSinkConfiguration = (LogSinkView) eventLogConfiguration.sink().value();
        LogSink logSink = new LogSink(logSinkConfiguration);
        Event event = IgniteEvents.USER_AUTHENTICATED.create(EventUser.of("user1", "basicProvider"));

        logSink.write(event);

        assertThat(Files.lines(eventlogFile.toPath()).collect(Collectors.toList()), Matchers.hasItem(event.toString()));
    }
}
