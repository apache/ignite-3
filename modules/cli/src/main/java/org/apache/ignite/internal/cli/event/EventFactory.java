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

package org.apache.ignite.internal.cli.event;

import jakarta.inject.Singleton;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.internal.logger.IgniteLogger;

/**
 * Register listeners and produces events.
 */
@Singleton
public class EventFactory {

    private static final IgniteLogger log = CliLoggers.forClass(EventFactory.class);

    /** All listeners. */
    private final ConcurrentHashMap<EventType, List<EventListener>> listeners = new ConcurrentHashMap<>();

    public EventFactory() {
    }

    /**
     * Registers an event listener.
     *
     * @param eventType     type of event to listen.
     * @param eventListener event listener.
     */
    public void listen(EventType eventType, EventListener eventListener) {
        listeners.computeIfAbsent(eventType, evtKey -> new CopyOnWriteArrayList<>()).add(eventListener);
    }

    /**
     * Removes a listener associated with the event type.
     *
     * @param eventType     type of event to listen.
     * @param eventListener event listener.
     */
    public void removeListener(EventType eventType, EventListener eventListener) {
        listeners.computeIfAbsent(eventType, eventT -> new CopyOnWriteArrayList<>()).remove(eventListener);
    }

    /**
     * Notifies every listener that subscribed before.
     *
     * @param eventType event type
     * @param event event itself.
     */
    public void fireEvent(EventType eventType, Event event) {
        List<EventListener> eventListeners = listeners.get(eventType);

        if (eventListeners == null) {
            return;
        }

        eventListeners.forEach(listener -> {
            try {
                listener.onEvent(eventType, event);
            } catch (Exception exception) {
                log.warn("Got an exception: ", exception);
            }
        });
    }
}
