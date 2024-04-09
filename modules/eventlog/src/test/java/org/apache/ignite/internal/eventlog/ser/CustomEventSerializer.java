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
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;

public class CustomEventSerializer extends JacksonBasedJsonSerializer {
    /** Default constructor. */
    public CustomEventSerializer() {
        super(createJacksonModule());
    }

    private static Module createJacksonModule() {
        SimpleModule module = new SimpleModule();
        module.addSerializer(CustomEvent.class, new CustomEventJacksonSerializer());
        module.addSerializer(Message.class, new MessageJacksonSerializer());
        return module;
    }

    static class CustomEventJacksonSerializer extends StdSerializer<CustomEvent> {
        CustomEventJacksonSerializer() {
            this(null);
        }

        CustomEventJacksonSerializer(Class<CustomEvent> e) {
            super(e);
        }

        @Override
        public void serialize(CustomEvent value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            jgen.writeStartObject();
            jgen.writeStringField("type", value.type());
            jgen.writeNumberField("timestamp", value.timestamp());
            jgen.writeStringField("productVersion", value.productVersion());
            jgen.writeObjectField("user", value.user());
            jgen.writeObjectField("message", value.message());
            jgen.writeObjectField("fields", value.fields());
            jgen.writeEndObject();
        }
    }

    static class MessageJacksonSerializer extends StdSerializer<Message> {
        MessageJacksonSerializer() {
            super((Class<Message>) null);
        }

        @Override
        public void serialize(Message value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            jgen.writeStartObject();
            jgen.writeNumberField("version", value.getVersion());
            jgen.writeStringField("body", value.getBody());
            jgen.writeEndObject();
        }
    }
}
