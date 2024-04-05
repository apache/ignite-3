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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.eventlog.api.Event;

public class EventSerializerRegistryImpl implements EventSerializerRegistry {
    private final ReadWriteLock guard = new ReentrantReadWriteLock();
    private final Map<Class<? extends Event>, EventSerializer> serializers = new HashMap<>();


    @Override
    public void register(Class<? extends Event> eventClass, EventSerializer eventSerializer) {
        guard.writeLock().lock();
        try {
            if (serializers.containsKey(eventClass)) {
                throw new IllegalStateException("EventSerializer for class " + eventClass.getCanonicalName() + " is already registered");
            } else {
                serializers.put(eventClass, eventSerializer);
            }
        } finally {
            guard.writeLock().unlock();
        }
    }

    @Override
    public EventSerializer findSerializer(Class<? extends Event> eventClass) {
        guard.readLock().lock();
        try {
            return serializers.get(eventClass);
        } finally {
            guard.readLock().unlock();
        }
    }
}
