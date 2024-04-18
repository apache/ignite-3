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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.apache.ignite.internal.eventlog.api.Event;
import org.apache.ignite.internal.eventlog.event.exception.InvalidEventTypeException;
import org.apache.ignite.internal.eventlog.event.exception.InvalidProductVersionException;
import org.apache.ignite.internal.eventlog.event.exception.MissingEventTypeException;
import org.apache.ignite.internal.eventlog.event.exception.MissingEventUserException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class EventBuilderTest {
    private static final String EVENT_TYPE = "TEST_EVENT_TYPE";

    @BeforeAll
    static void beforeAll() {
        // Register test event type in order to be able to create events with it.
        EventTypeRegistry.register(EVENT_TYPE);
    }

    @Test
    void buildPositive() {
        Event event = Event.builder()
                .type(EVENT_TYPE)
                .timestamp(1)
                .productVersion("1.1.1")
                .user(EventUser.system())
                .fields(Map.of("key", "value"))
                .build();

        assertEquals(EVENT_TYPE, event.getType());
        assertEquals(1, event.getTimestamp());
        assertEquals("1.1.1", event.getProductVersion());
        assertEquals(EventUser.system(), event.getUser());
        assertEquals(Map.of("key", "value"), event.getFields());
    }

    @Test
    void buildWithoutFields() {
        Event event = Event.builder()
                .type(EVENT_TYPE)
                .timestamp(1)
                .productVersion("1.1.1")
                .user(EventUser.system())
                .build();

        assertEquals(EVENT_TYPE, event.getType());
        assertEquals(1, event.getTimestamp());
        assertEquals("1.1.1", event.getProductVersion());
        assertEquals(EventUser.system(), event.getUser());
        assertEquals(Map.of(), event.getFields());
    }

    @Test
    void buildWithDefaults() {
        Event event = Event.builder()
                .type(EVENT_TYPE)
                .user(EventUser.system())
                .build();

        assertEquals(EVENT_TYPE, event.getType());
        assertThat(event.getTimestamp(), greaterThan(0L));
        assertEquals("3.0.0", event.getProductVersion());
        assertEquals(EventUser.system(), event.getUser());
        assertEquals(Map.of(), event.getFields());
    }

    @Test
    void buildIncorrectType() {
        assertThrows(
                InvalidEventTypeException.class,
                () -> Event.builder()
                        .type("INCORRECT_TYPE")
                        .timestamp(1)
                        .productVersion("1.1.1")
                        .user(EventUser.system())
                        .build(),
                "Got invalid event type `INCORRECT_TYPE` during event creation. "
                        + "If you want to use `INCORRECT_TYPE`, register it in EventTypeRegistry."
        );
    }

    @Test
    void typeFieldIsRequired() {
        assertThrows(
                MissingEventTypeException.class,
                () -> Event.builder().build(),
                "Missing event type during event creation."
        );
    }

    @Test
    void userFieldIsRequired() {
        assertThrows(
                MissingEventUserException.class,
                () -> Event.builder()
                        .type(EVENT_TYPE)
                        .timestamp(1)
                        .productVersion("1.1.1")
                        .build(),
                "Missing event user during event creation. If there is no user, use `EventUser.system()`"
        );
    }

    @ParameterizedTest
    @CsvSource("asdf, 1111, 1, 1.a.1, 1, 1.1.a, some-1.1.1")
    void invalidVersion(String version) {
        assertThrows(
                InvalidProductVersionException.class,
                () -> Event.builder()
                        .type(EVENT_TYPE)
                        .timestamp(1)
                        .productVersion(version)
                        .user(EventUser.system())
                        .build()
        );
    }
}
