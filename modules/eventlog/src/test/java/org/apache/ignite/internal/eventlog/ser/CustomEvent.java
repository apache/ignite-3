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

import java.util.Map;
import org.apache.ignite.internal.eventlog.api.Event;
import org.apache.ignite.internal.eventlog.event.EventUser;

class CustomEvent implements Event {
    private final long timestamp;
    private final String productVersion;
    private final EventUser eventUser;
    private final Message message;

    CustomEvent(long timestamp, String productVersion, EventUser eventUser, Message message) {
        this.timestamp = timestamp;
        this.productVersion = productVersion;
        this.eventUser = eventUser;
        this.message = message;
    }

    @Override
    public String getType() {
        return "CUSTOM";
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
        return eventUser;
    }

    @Override
    public Map<String, Object> getFields() {
        return Map.of(
                "hasMessage", message != null
        );
    }

    public Message getMessage() {
        return message;
    }
}
