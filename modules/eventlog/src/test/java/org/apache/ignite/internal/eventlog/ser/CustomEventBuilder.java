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

import org.apache.ignite.internal.eventlog.event.EventUser;

class CustomEventBuilder {
    private long timestamp;
    private String productVersion;
    private EventUser eventUser;
    private Message message;

    public CustomEventBuilder timestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public CustomEventBuilder productVersion(String productVersion) {
        this.productVersion = productVersion;
        return this;
    }

    public CustomEventBuilder user(EventUser eventUser) {
        this.eventUser = eventUser;
        return this;
    }

    public CustomEventBuilder message(Message message) {
        this.message = message;
        return this;
    }

    public CustomEvent build() {
        return new CustomEvent(timestamp, productVersion, eventUser, message);
    }
}
