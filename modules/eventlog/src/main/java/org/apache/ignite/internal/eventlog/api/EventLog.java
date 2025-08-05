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

import java.util.function.Supplier;

/**
 * The main interface for logging events.
 *
 * <p>Example of usage. Let it be configured in the cluster configuration:
 * <pre>
 *     eventlog.channels.exampleChannel: {
 *       types: [USER_AUTHENTICATED],
 *       enabled: true
 *     }
 *     eventlog.sinks.exampleSink: {
 *       channel: "exampleChannel",
 *       type: "log",
 *       criteria: "exampleLog"
 *     }
 * </pre>
 *
 * <p>Here is how to fire an event that will be logged into the log file defined by "exampleLog":
 * <pre>
 *     eventLog.log(() -> IgniteEvents.USER_AUTHENTICATED.create(EventUser.of("user1", "basicAuthenticationProvider"));
 * </pre>
 */
public interface EventLog {
    /**
     * Writes event into every channel this event relates to.
     *
     * @param event The event to log.
     */
    void log(Event event);

    /**
     * Lazy version of {@link #log(Event)}. It first checks if the events of the given type are enabled and then logs the event. INVARIANT:
     * type must be a valid event type and eventProvider must provide an event of the same type.
     *
     * @param type the type of the event.
     * @param eventProvider the event provider.
     */
    void log(String type, Supplier<Event> eventProvider);

    EventLog NOOP = new EventLog() {
        @Override
        public void log(Event event) {
            // No-op.
        }

        @Override
        public void log(String type, Supplier<Event> eventProvider) {
            // No-op.
        }
    };

}
