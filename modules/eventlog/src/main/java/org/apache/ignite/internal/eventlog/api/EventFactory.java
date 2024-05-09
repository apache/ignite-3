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
 * The factory that is responsible for creating events. This interface should be used everywhere where events are created. Only special
 * cases should use {@link EventBuilder} directly, for example, in tests.
 */
public interface EventFactory {
    /**
     * Creates an event object.
     *
     * @param user The user that caused the event.
     * @return The event object.
     */
    Event create(EventUser user);

    /**
     * Creates an event builder with the event type defined. The type is set by the factory. For example,
     * {@link IgniteEvents.CONNECTION_CLOSED.build} will return a builder with {@link IgniteEventType.CONNECTION_CLOSED} type set.
     */
    EventBuilder builder();
}
