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
import static org.hamcrest.Matchers.equalTo;

import java.util.stream.Stream;
import org.apache.ignite.internal.eventlog.api.Event;
import org.apache.ignite.internal.eventlog.api.IgniteEvents;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class IgniteEventsTest {
    private static final String USER = "test_user";

    private static final String PROVIDER = "test_provider";

    private static Stream<Arguments> events() {
        Event connectionClosedEvent = IgniteEvents.CLIENT_CONNECTION_CLOSED.create(EventUser.of(USER, PROVIDER));
        Event connectionEstablishedEvent = IgniteEvents.USER_AUTHENTICATION_SUCCESS.create(EventUser.of(USER, PROVIDER));

        return Stream.of(
                Arguments.of(
                        connectionClosedEvent,
                        Event.builder()
                                .type("CLIENT_CONNECTION_CLOSED")
                                .productVersion(IgniteProductVersion.CURRENT_VERSION.toString())
                                .timestamp(connectionClosedEvent.getTimestamp())
                                .user(EventUser.of(USER, PROVIDER))
                                .build()
                ),
                Arguments.of(
                        connectionEstablishedEvent,
                        Event.builder()
                                .type("USER_AUTHENTICATION_SUCCESS")
                                .productVersion(IgniteProductVersion.CURRENT_VERSION.toString())
                                .timestamp(connectionEstablishedEvent.getTimestamp())
                                .user(EventUser.of(USER, PROVIDER))
                                .build()
                )
        );
    }

    @ParameterizedTest
    @MethodSource("events")
    void createEvents(Event givenEvent, Event expectedEvent) {
        assertThat(givenEvent, equalTo(expectedEvent));
    }
}
