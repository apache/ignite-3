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

package org.apache.ignite.internal.eventlog.event;

import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.eventlog.api.EventLog;
import org.apache.ignite.internal.eventlog.api.EventLogImpl;
import org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EventLogTest {
    private static final EventUser TEST_USER = EventUser.of("testuser", "basicAuthenticator");
    @InjectConfiguration
    private EventLogConfiguration cfg;

    private EventLog eventLog;

    @BeforeEach
    void setUp() {
       eventLog = new EventLogImpl(cfg);
    }

    @Test
    void logEvents() {
        // When log event with default configuration.
        eventLog.log(() -> IgniteEvents.USER_AUTHENTICATED.create(TEST_USER));

        // Then nothing happens.

    }
}
