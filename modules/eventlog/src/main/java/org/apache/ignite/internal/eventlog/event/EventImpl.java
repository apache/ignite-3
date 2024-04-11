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
import java.util.Objects;
import org.apache.ignite.internal.eventlog.api.Event;

/**
 * Implementation of the {@link Event} interface. The class is immutable and thread-safe. If you want to create an instance of this class,
 * use the {@link EventBuilder}.
 *
 * <p>NOTE: This class should always be a plain POJO.
 */
public class EventImpl implements Event {
    private final String type;

    private final long timestamp;

    private final String productVersion;

    private final EventUser user;

    private final Map<String, Object> fields;

    EventImpl(String type, long timestamp, String productVersion, EventUser user, Map<String, Object> fields) {
        this.type = type;
        this.timestamp = timestamp;
        this.productVersion = productVersion;
        this.user = user;
        this.fields = fields;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String getProductVersion() {
        return productVersion;
    }

    @Override
    public EventUser getUser() {
        return user;
    }

    @Override
    public Map<String, Object> getFields() {
        return fields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EventImpl event = (EventImpl) o;
        return timestamp == event.timestamp && Objects.equals(type, event.type) && Objects.equals(productVersion,
                event.productVersion) && Objects.equals(user, event.user) && Objects.equals(fields, event.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, timestamp, productVersion, user, fields);
    }

    @Override
    public String toString() {
        return "EventImpl{"
                + "type='" + type + '\''
                + ", timestamp=" + timestamp
                + ", productVersion='" + productVersion + '\''
                + ", user=" + user
                + ", fields=" + fields
                + '}';
    }
}
