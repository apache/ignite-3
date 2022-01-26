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

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CopyOnWriteArrayList;
import org.jetbrains.annotations.Nullable;

/**
 * Holder (thread safe) for configuration change listeners.
 */
class ConfigurationListenerHolder<L> {
    private final Collection<Container<L>> containers = new CopyOnWriteArrayList<>();

    /**
     * Adds a listener.
     *
     * @param listener Configuration change listener.
     * @param storageRevision Storage revision after which the listener will be called.
     * @see ConfigurationListenerHolder#listeners
     */
    void addListener(L listener, long storageRevision) {
        containers.add(new Container<>(listener, storageRevision));
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
                return listener == ((Container) obj).listener;
            }
        });
    }

    /**
     * Returns an iterator of the listeners for the {@code storageRevision} (were added for and before it).
     *
     * <p>NOTE: {@link Iterator#remove} - not supported.
     *
     * @param storageRevision Storage revision.
     * @see ConfigurationListenerHolder#addListener
     */
    Iterator<L> listeners(long storageRevision) {
        Iterator<Container<L>> it = containers.iterator();

        return new Iterator<L>() {
            @Nullable L curr = advance();

            /** {@inheritDoc} */
            @Override
            public boolean hasNext() {
                return curr != null;
            }

            /** {@inheritDoc} */
            @Override
            public L next() {
                L next = curr;

                if (next == null) {
                    throw new NoSuchElementException();
                }

                curr = advance();

                return next;
            }

            @Nullable L advance() {
                while (it.hasNext()) {
                    Container<L> next = it.next();

                    if (next.storageRevision <= storageRevision) {
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

        final long storageRevision;

        Container(L listener, long storageRevision) {
            this.listener = listener;
            this.storageRevision = storageRevision;
        }
    }
}
