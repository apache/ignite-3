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

package org.apache.ignite.internal.manager;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * A class which can produce its events.
 */
public abstract class Producer<T extends Event, P extends EventParameters> {
    /** All listeners. */
    private ConcurrentHashMap<T, List<EventListener<P>>> listeners = new ConcurrentHashMap<>();

    /**
     * Registers an event listener. When the event predicate returns true it would never invoke after, otherwise this predicate would
     * receive an event again.
     *
     * @param evt     Event.
     * @param closure Closure.
     */
    public void listen(T evt, EventListener<P> closure) {
        listeners.computeIfAbsent(evt, evtKey -> new CopyOnWriteArrayList<>()).add(closure);
    }

    /**
     * Removes a listener associated with the event.
     *
     * @param evt     Event.
     * @param closure Closure.
     */
    public void removeListener(T evt, EventListener<P> closure) {
        removeListener(evt, closure, null);
    }

    /**
     * Removes a listener associated with the event.
     *
     * @param evt     Event.
     * @param closure Closure.
     * @param cause   The exception that was a cause which a listener is removed.
     */
    public void removeListener(T evt, EventListener<P> closure, @Nullable IgniteInternalCheckedException cause) {
        removeListener(evt, closure, cause, true);
    }

    /**
     * Removes a listener associated with the event.
     *
     * @param evt          Event.
     * @param closure      Closure.
     * @param cause        The exception that was a cause which a listener is removed.
     * @param callOnRemove Whether to call {@link EventListener#remove(Throwable)} callback on the closure.
     */
    private void removeListener(T evt, EventListener<P> closure, @Nullable IgniteInternalCheckedException cause, boolean callOnRemove) {
        if (listeners.computeIfAbsent(evt, evtKey -> new CopyOnWriteArrayList<>()).remove(closure) && callOnRemove) {
            closure.remove(cause == null ? new ListenerRemovedException() : cause.getCause() == null ? cause : cause.getCause());
        }
    }

    /**
     * Notifies every listener that subscribed before.
     *
     * @param evt    Event type.
     * @param params Event parameters.
     * @param err    Exception when it was happened, or {@code null} otherwise.
     * @return Completable future which is completed when event handling is complete.
     */
    protected CompletableFuture<?> fireEvent(T evt, P params, Throwable err) {
        List<EventListener<P>> eventListeners = listeners.get(evt);

        if (eventListeners == null) {
            return completedFuture(null);
        }

        Collection<CompletableFuture<?>> futures = new ArrayList<>();

        Iterator<EventListener<P>> iter = eventListeners.iterator();

        while (iter.hasNext()) {
            EventListener<P> closure = iter.next();

            CompletableFuture<?> future = closure.notify(params, err)
                    .thenAccept(b -> {
                        if (b) {
                            removeListener(evt, closure, null, false);
                        }
                    });

            futures.add(future);
        }

        return allOf(futures.toArray(new CompletableFuture[0]));
    }

    /**
     * Notifies every listener that subscribed before.
     *
     * @param evt    Event type.
     * @param params Event parameters.
     * @return Completable future which is completed when event handling is complete.
     */
    protected  CompletableFuture<?> fireEvent(T evt, P params) {
        return fireEvent(evt, params, null);
    }
}
