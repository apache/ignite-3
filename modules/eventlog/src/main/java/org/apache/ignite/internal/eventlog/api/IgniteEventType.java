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

import java.util.Arrays;
import org.apache.ignite.internal.eventlog.event.EventBuilder;
import org.apache.ignite.internal.eventlog.event.EventTypeRegistry;
import org.apache.ignite.internal.eventlog.event.EventUser;

/**
 * Defines a subset of event types that can be created in the system. Note, the event type is a string that is unique within the system. The
 * event type is used to filter the events in the event log.
 *
 * <p>If you want to create an instance of the Event with the specified type, use the {@link #create} method.
 *
 * <p>For example, to create an event of the type USER_AUTHENTICATED:
 * <pre>{@code IgniteEventType.USER_AUTHENTICATION_SUCCESS.create(EventUser.system());}</pre>
 */
public enum IgniteEventType {
    USER_AUTHENTICATION_SUCCESS,
    USER_AUTHENTICATION_FAILURE,
    CLIENT_CONNECTION_ESTABLISHED,
    CLIENT_CONNECTION_CLOSED,

    COMPUTE_JOB_QUEUED,
    COMPUTE_JOB_EXECUTING,
    COMPUTE_JOB_FAILED,
    COMPUTE_JOB_COMPLETED,
    COMPUTE_JOB_CANCELING,
    COMPUTE_JOB_CANCELED,

    COMPUTE_TASK_QUEUED,
    COMPUTE_TASK_EXECUTING,
    COMPUTE_TASK_FAILED,
    COMPUTE_TASK_COMPLETED,
    COMPUTE_TASK_CANCELED,

    QUERY_STARTED,
    QUERY_FINISHED;

    static {
        // Without the following line, the IgniteEventType enum will not be registered in the EventTypeRegistry
        // and the EventTypeRegistry will not be able to validate the event types.
        Arrays.stream(values()).forEach(type -> EventTypeRegistry.register(type.name()));
    }

    /**
     * Registers all event types through the static initialization block once.
     */
    public static void initialize() {
    }

    /**
     * Creates an event of this type with current timestamp and specified event user.
     *
     * @param user Event user.
     * @return Created event.
     */
    public Event create(EventUser user) {
        return builder()
                .user(user)
                .timestamp(System.currentTimeMillis())
                .build();
    }

    /**
     * Creates new event build of this type.
     *
     * @return Created event builder.
     */
    public EventBuilder builder() {
        return Event.builder()
                .type(name());
    }
}
