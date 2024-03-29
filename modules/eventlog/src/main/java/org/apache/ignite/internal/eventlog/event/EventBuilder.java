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

import java.util.Map;
import java.util.regex.Pattern;
import org.apache.ignite.internal.eventlog.api.Event;
import org.apache.ignite.internal.eventlog.api.EventFactory;
import org.apache.ignite.internal.eventlog.event.exception.InvalidEventTypeException;
import org.apache.ignite.internal.eventlog.event.exception.InvalidProductVersionException;
import org.apache.ignite.internal.eventlog.event.exception.MissingEventTypeException;
import org.apache.ignite.internal.eventlog.event.exception.MissingEventUserException;

/**
 * The event builder that should be used only where the {@link EventFactory} is not enough to create an event. For example, in tests, where
 * you want to put some special event. In all other cases, use {@link EventFactory} to create events.
 */
public class EventBuilder {
    // TODO: https://issues.apache.org/jira/browse/IGNITE-21812
    private static final String DEFAULT_VERSION = "3.0.0";

    private static final Pattern SEMVER_REGEX = Pattern.compile(
            "(?<major>\\d+)\\.(?<minor>\\d+)\\.(?<maintenance>\\d+)"
                    + "((?<snapshot>-SNAPSHOT)|-(?<alpha>alpha\\d+)|--(?<beta>beta\\d+)|---(?<ea>ea\\d+))?");

    private String type;

    private long timestamp;

    private String productVersion;

    private EventUser user;

    private Map<String, Object> fields;

    public EventBuilder() {
    }

    /** The type of the event. The type must be not null and registered in the {@link EventTypeRegistry}. */
    public EventBuilder type(String type) {
        this.type = type;
        return this;
    }

    /** The unix timestamp of the event. If not set, the {@link System#currentTimeMillis()}. */
    public EventBuilder timestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    /** The product version. The version must be compatible with vemver. */
    public EventBuilder productVersion(String productVersion) {
        this.productVersion = productVersion;
        return this;
    }

    /**
     * The user that caused the event. The user must set explicitly, otherwise the exception will be thrown in {@link #build()}.
     */
    public EventBuilder user(EventUser user) {
        this.user = user;
        return this;
    }

    /** The event-specific fields of the event. If not set, the empty map will be set to the event object. */
    public EventBuilder fields(Map<String, Object> fields) {
        this.fields = fields;
        return this;
    }

    /**
     * Verifies all invariants for the {@link Event} first. If all invariants are satisfied, creates the event object. If not, throws one of
     * the following exceptions: {@link MissingEventTypeException}, {@link InvalidEventTypeException}, or
     * {@link MissingEventUserException}.
     *
     * @return The valid event object.
     */
    public Event build() {
        if (type == null) {
            throw new MissingEventTypeException();
        }

        if (!EventTypeRegistry.contains(type)) {
            throw new InvalidEventTypeException(type);
        }

        if (user == null) {
            throw new MissingEventUserException();
        }

        if (productVersion == null) {
            productVersion = DEFAULT_VERSION;
        }

        if (!SEMVER_REGEX.matcher(productVersion).matches()) {
            throw new InvalidProductVersionException(productVersion);
        }

        if (fields == null) {
            fields = Map.of();
        }

        if (timestamp == 0) {
            timestamp = System.currentTimeMillis();
        }

        return new EventImpl(type, timestamp, productVersion, user, fields);
    }
}
