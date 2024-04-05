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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import org.apache.ignite.internal.eventlog.event.EventImpl;

/** Serializes events to JSON. */
public class JsonEventImplSerializer extends JacksonBasedJsonSerializer<EventImpl> {
    /** Default constructor. */
    public JsonEventImplSerializer() {
        super(EventImpl.class, new EventImplJacksonSerializer());
    }

    static class EventImplJacksonSerializer extends StdSerializer<EventImpl> {
        private EventImpl value;
        private JsonGenerator jgen;
        private SerializerProvider provider;

        EventImplJacksonSerializer() {
            this(null);
        }

        EventImplJacksonSerializer(Class<EventImpl> e) {
            super(e);
        }

        @Override
        public void serialize(EventImpl value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            this.value = value;
            this.jgen = jgen;
            this.provider = provider;

            jgen.writeStartObject();
            jgen.writeStringField("type", value.type());
            jgen.writeNumberField("timestamp", value.timestamp());
            jgen.writeStringField("productVersion", value.productVersion());
            jgen.writeObjectField("user", value.user());
            jgen.writeObjectField("fields", value.fields());
            jgen.writeEndObject();
        }
    }
}
