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

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.Collections;
import java.util.Map;
import org.apache.ignite.internal.eventlog.api.Event;
import org.apache.ignite.internal.eventlog.event.EventImpl;
import org.apache.ignite.internal.eventlog.event.EventUser;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class EventSerializerRegistryImplTest {
    @Test public void youCanRegisterAndThenFindSerializer() {
        EventSerializerRegistry eventSerializerRegistry = new EventSerializerRegistryImpl();
        EventSerializer customEventSerializer = Mockito.mock(EventSerializer.class);
        eventSerializerRegistry.register(CustomEvent.class, customEventSerializer);

        EventSerializer registeredSerializer = eventSerializerRegistry.findSerializer(CustomEvent.class);
        assertSame(customEventSerializer, registeredSerializer);
    }

    @Test public void youCanRegisterSameSerializerForMultipleTypes() {
        EventSerializerRegistry eventSerializerRegistry = new EventSerializerRegistryImpl();
        EventSerializer customEventSerializer = Mockito.mock(EventSerializer.class);
        eventSerializerRegistry.register(CustomEvent.class, customEventSerializer);
        eventSerializerRegistry.register(EventImpl.class, customEventSerializer);

        EventSerializer registeredCustomEventSerializer = eventSerializerRegistry.findSerializer(CustomEvent.class);
        assertSame(customEventSerializer, registeredCustomEventSerializer);
        EventSerializer registeredEventImplSerializer = eventSerializerRegistry.findSerializer(EventImpl.class);
        assertSame(customEventSerializer, registeredEventImplSerializer);
    }

    @Test public void youCantRegisterMultipleSerializersForSameEventClass() {
        EventSerializerRegistry eventSerializerRegistry = new EventSerializerRegistryImpl();
        EventSerializer customEventSerializer = Mockito.mock(EventSerializer.class);
        eventSerializerRegistry.register(CustomEvent.class, customEventSerializer);
        eventSerializerRegistry.register(EventImpl.class, customEventSerializer);

        EventSerializer registeredCustomEventSerializer = eventSerializerRegistry.findSerializer(CustomEvent.class);
        assertSame(customEventSerializer, registeredCustomEventSerializer);
        EventSerializer registeredEventImplSerializer = eventSerializerRegistry.findSerializer(EventImpl.class);
        assertSame(customEventSerializer, registeredEventImplSerializer);
    }

    @Test public void nullIsReturnedIfSerializerIsNotRegistered() {
        EventSerializerRegistry eventSerializerRegistry = new EventSerializerRegistryImpl();
        EventSerializer eventSerializer = eventSerializerRegistry.findSerializer(CustomEvent.class);
        assertNull(eventSerializer);
    }

    static class CustomEvent implements Event {

        @Override
        public String type() {
            return "Custom";
        }

        @Override
        public long timestamp() {
            return 0;
        }

        @Override
        public String productVersion() {
            return "0.1";
        }

        @Override
        public EventUser user() {
            return EventUser.system();
        }

        @Override
        public Map<String, Object> fields() {
            return Collections.emptyMap();
        }
    }
}
