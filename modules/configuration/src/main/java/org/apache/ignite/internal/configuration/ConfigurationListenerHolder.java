/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.configuration;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CopyOnWriteArrayList;
import org.jetbrains.annotations.Nullable;

/**
 * Holder (thread safe) for configuration change listeners.
 */
class ConfigurationListenerHolder<L> {
    private static final Object NULL_LISTENER = new Object();

    private final List<Container<L>> containers = new CopyOnWriteArrayList<>();

    /**
     * Adds a listener.
     *
     * @param listener Configuration change listener.
     * @param notificationNumber Configuration notification listener number after which the listener will be called.
     * @throws IllegalArgumentException If {@code notificationNumber} less than zero.
     * @see ConfigurationListenerHolder#listeners
     */
    void addListener(L listener, long notificationNumber) {
        if (notificationNumber < 0) {
            throw new IllegalArgumentException("notificationNumber must be greater than or equal to 0");
        }

        containers.add(new Container<>(listener, notificationNumber + 1));
    }

    /**
     * Removes the listener.
     *
     * @param listener Configuration change listener.
     */
    void removeListener(L listener) {
        containers.remove(new Container<>(listener, -1) {
            /** {@inheritDoc} */
            @Override
            public boolean equals(Object obj) {
                Container<L> container = (Container<L>) obj;

                if (listener == container.listener) {
                    container.notificationNumber = -1;

                    return true;
                } else {
                    return false;
                }
            }
        });
    }

    /**
     * Returns an iterator of the listeners for the {@code notificationNumber} (were added for and before it).
     *
     * <p>NOTE: {@link Iterator#remove} - not supported.
     *
     * @param notificationNumber Configuration notification listener number.
     * @throws IllegalArgumentException If {@code notificationNumber} less than zero.
     * @see ConfigurationListenerHolder#addListener
     */
    Iterator<L> listeners(long notificationNumber) {
        if (notificationNumber < 0) {
            throw new IllegalArgumentException("notificationNumber must be greater than or equal to 0");
        }

        Iterator<Container<L>> it = containers.iterator();

        return new Iterator<L>() {
            @Nullable L curr = (L) NULL_LISTENER;

            /** {@inheritDoc} */
            @Override
            public boolean hasNext() {
                if (curr == NULL_LISTENER) {
                    curr = advance();
                }

                return curr != null;
            }

            /** {@inheritDoc} */
            @Override
            public L next() {
                if (curr == NULL_LISTENER) {
                    curr = advance();
                }

                if (curr == null) {
                    throw new NoSuchElementException();
                }

                L next = curr;

                curr = (L) NULL_LISTENER;

                return next;
            }

            @Nullable L advance() {
                while (it.hasNext()) {
                    Container<L> next = it.next();

                    long number = next.notificationNumber;

                    // Negative number means the container has been removed.
                    if (number >= 0 && number <= notificationNumber) {
                        return next.listener;
                    }
                }

                return null;
            }
        };
    }

    /**
     * Removes all listeners.
     */
    void clear() {
        containers.clear();
    }

    /**
     * Configuration change listener container.
     */
    private static class Container<L> {
        final L listener;

        volatile long notificationNumber;

        Container(L listener, long storageRevision) {
            this.listener = listener;
            this.notificationNumber = storageRevision;
        }
    }
}
