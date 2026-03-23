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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.eventlog.api.IgniteEventType;
import org.apache.ignite.internal.eventlog.event.exception.NotUniqueEventTypeException;

/**
 * Registry of all event types that are known to the system. Every new event type must be registered in this registry once. For the example
 * of usage, see {@link IgniteEventType}. The class is thread-safe.
 */
public final class EventTypeRegistry {
    private static final Object DUMMY = new Object();

    private static final ConcurrentHashMap<String, Object> allTypes = new ConcurrentHashMap<>();

    private EventTypeRegistry() {
    }

    /** Registers a set of event types. */
    public static void register(Set<String> types) {
        new HashSet<>(types).forEach(EventTypeRegistry::register);
    }

    /** Registers an event type. */
    public static void register(String type) {
        allTypes.compute(type, (k, v) -> {
            if (v != null) {
                throw new NotUniqueEventTypeException(type);
            }
            return DUMMY;
        });
    }

    /** Checks if the event type is registered. */
    public static boolean contains(String type) {
        return allTypes.containsKey(type);
    }
}
