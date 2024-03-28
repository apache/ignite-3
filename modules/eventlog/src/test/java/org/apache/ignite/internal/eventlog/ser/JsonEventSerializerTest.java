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

package org.apache.ignite.internal.eventlog.ser;

import static org.hamcrest.MatcherAssert.assertThat;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONAs;

import java.util.Map;
import java.util.stream.Stream;
import org.apache.ignite.internal.eventlog.api.Event;
import org.apache.ignite.internal.eventlog.event.EventUser;
import org.apache.ignite.internal.eventlog.event.IgniteEvents;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class JsonEventSerializerTest {
    public static Stream<Arguments> events() {
        return Stream.of(
                Arguments.of(
                        IgniteEvents.CONNECTION_CLOSED.builder()
                                .productVersion("3.0.0")
                                .timestamp(1234567890)
                                .user(EventUser.of("test_user", "test_provider"))
                                .build(),
                        "{\"type\":\"CONNECTION_CLOSED\","
                                + "\"timestamp\":1234567890,"
                                + "\"productVersion\":\"3.0.0\","
                                + "\"user\":{\"username\":\"test_user\",\"authenticationProvider\":\"test_provider\"},"
                                + "\"fields\":{}"
                                + "}"
                ),
                Arguments.of(
                        IgniteEvents.USER_AUTHENTICATED.builder()
                                .productVersion("3.0.0")
                                .timestamp(1234567890)
                                .user(EventUser.of("test_user", "test_provider"))
                                .build(),
                        "{\"type\":\"USER_AUTHENTICATED\","
                                + "\"timestamp\":1234567890,"
                                + "\"productVersion\":\"3.0.0\","
                                + "\"user\":{\"username\":\"test_user\",\"authenticationProvider\":\"test_provider\"},"
                                + "\"fields\":{}"
                                + "}"
                ),
                Arguments.of(
                        IgniteEvents.USER_AUTHENTICATED.builder()
                                .productVersion("3.0.0")
                                .timestamp(1234567890)
                                .user(EventUser.of("test_user", "test_provider"))
                                .fields(Map.of("ip", "127.0.0.1", "id", "123"))
                                .build(),
                        "{\"type\":\"USER_AUTHENTICATED\","
                                + "\"timestamp\":1234567890,"
                                + "\"productVersion\":\"3.0.0\","
                                + "\"user\":{\"username\":\"test_user\",\"authenticationProvider\":\"test_provider\"},"
                                + "\"fields\":{\"id\":\"123\",\"ip\":\"127.0.0.1\"}"
                                + "}"
                )
        );
    }

    private JsonEventSerializer serializer;

    @BeforeEach
    void setUp() {
        serializer = new JsonEventSerializer();
    }

    @ParameterizedTest
    @MethodSource("events")
    void serialize(Event givenEvent, String expectedString) {
        String serialized = serializer.serialize(givenEvent);
        assertThat(serialized, sameJSONAs(expectedString));
    }
}
