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

package org.apache.ignite.internal.eventlog.api;

import org.apache.ignite.internal.eventlog.event.EventBuilder;
import org.apache.ignite.internal.eventlog.event.EventUser;

/**
 * The main class for creating all Ignite events.
 *
 * <p>If you want to create an instance of the Event with the specified type, use the {@link #create} method.
 *
 * <p>For example, to create an event of the type USER_AUTHENTICATED:
 * <pre>{@code IgniteEvents.USER_AUTHENTICATED.create(EventUser.system());}</pre>
 */
public final class IgniteEvents implements EventFactory {
    public static final IgniteEvents USER_AUTHENTICATED = new IgniteEvents(IgniteEventType.USER_AUTHENTICATED.name());

    public static final IgniteEvents CONNECTION_CLOSED = new IgniteEvents(IgniteEventType.CONNECTION_CLOSED.name());

    private final String type;

    private IgniteEvents(String type) {
        this.type = type;
    }

    @Override
    public Event create(EventUser user) {
        return Event.builder()
                .type(type)
                .user(user)
                .timestamp(System.currentTimeMillis())
                .build();
    }

    @Override
    public EventBuilder builder() {
        return new EventBuilder().type(type);
    }
}
