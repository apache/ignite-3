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

import java.util.Map;
import org.apache.ignite.internal.eventlog.event.EventBuilder;
import org.apache.ignite.internal.eventlog.event.EventTypeRegistry;
import org.apache.ignite.internal.eventlog.event.EventUser;

/**
 * Represents an event object that can be logged to the event log.
 * All implementations of this interface must be plain POJO.
 **/
public interface Event {
    /** Default builder for the event object. */
    static EventBuilder builder() {
        return new EventBuilder();
    }

    /** The type of the event. The type must be registered in the {@link EventTypeRegistry}. */
    String getType();

    /** The unix timestamp of the event. */
    long getTimestamp();

    /** The product version. The version is compatible with semver. */
    String getProductVersion();

    /** The user that caused the event. If the user is not available, the method returns a system user. */
    EventUser getUser();

    /** The event-specific fields of the event. */
    Map<String, Object> getFields();
}
