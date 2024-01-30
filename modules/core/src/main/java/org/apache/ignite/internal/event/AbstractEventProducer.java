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

package org.apache.ignite.internal.event;

import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.jetbrains.annotations.Nullable;

/**
 * Event producer.
 *
 * <p>Allows to {@link #listen add} and {@link #removeListener remove} event listeners for events, as well as
 * {@link #fireEvent fire events}.</p>
 */
public abstract class AbstractEventProducer<T extends Event, P extends EventParameters> implements EventProducer<T, P> {
    private final ConcurrentHashMap<T, List<EventListener<P>>> listenersByEvent = new ConcurrentHashMap<>();

    @Override
    public void listen(T evt, EventListener<? extends P> listener) {
        listenersByEvent.compute(evt, (evt0, listeners) -> {
            List<EventListener<P>> newListeners;

            if (listeners == null) {
                newListeners = new ArrayList<>(1);
            } else {
                newListeners = new ArrayList<>(listeners.size() + 1);

                newListeners.addAll(listeners);
            }

            newListeners.add((EventListener<P>) listener);

            return unmodifiableList(newListeners);
        });
    }

    @Override
    public void removeListener(T evt, EventListener<? extends P> listener) {
        listenersByEvent.computeIfPresent(evt, (evt0, listeners) -> {
            var newListeners = new ArrayList<>(listeners);

            newListeners.remove(listener);

            return newListeners.isEmpty() ? null : unmodifiableList(newListeners);
        });
    }

    /**
     * Notifies every listener that subscribed before.
     *
     * @param evt Event.
     * @param params Event parameters.
     * @param err Exception when it was happened, or {@code null} otherwise.
     * @return Completable future which is completed when event handling is complete.
     */
    protected CompletableFuture<Void> fireEvent(T evt, P params, @Nullable Throwable err) {
        List<EventListener<P>> listeners = listenersByEvent.get(evt);

        if (listeners == null) {
            return nullCompletedFuture();
        }

        // Lazy init.
        List<CompletableFuture<?>> futures = null;

        for (int i = 0; i < listeners.size(); i++) {
            EventListener<P> listener = listeners.get(i);

            CompletableFuture<Boolean> future = listener.notify(params, err);

            if (future.isDone() && !future.isCompletedExceptionally()) {
                if (future.join()) {
                    removeListener(evt, listener);
                }
            } else {
                if (futures == null) {
                    futures = new ArrayList<>();
                }

                futures.add(future.thenAccept(remove -> {
                    if (remove) {
                        removeListener(evt, listener);
                    }
                }));
            }
        }

        return futures == null ? nullCompletedFuture() : allOf(futures.toArray(CompletableFuture[]::new));
    }

    /**
     * Notifies every listener that subscribed before.
     *
     * @param evt Event.
     * @param params Event parameters.
     * @return Completable future which is completed when event handling is complete.
     */
    protected CompletableFuture<Void> fireEvent(T evt, P params) {
        return fireEvent(evt, params, null);
    }
}
