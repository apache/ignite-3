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

package org.apache.ignite.internal.configuration;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CopyOnWriteArrayList;
import org.jetbrains.annotations.Nullable;

/**
 * Holder (thread safe) for configuration change listeners.
 */
public class ConfigurationListenerHolder<L> {
    private final List<Container<L>> containers = new CopyOnWriteArrayList<>();

    /**
     * Adds a listener.
     *
     * @param listener Configuration change listener.
     * @param notificationNumber Configuration notification listener number after which the listener will be called.
     * @see ConfigurationListenerHolder#listeners
     */
    public void addListener(L listener, long notificationNumber) {
        containers.add(new Container<>(listener, notificationNumber + 1));
    }

    /**
     * Removes the listener.
     *
     * <p>NOTE: This method introduces unpredictable behavior at the moment, because the final behavior of this method is unclear.
     * Should the listener be removed immediately or only on the next notification? We'll fix it later if there's a problem.
     *
     * @param listener Configuration change listener.
     */
    public void removeListener(L listener) {
        containers.remove(new Container<>(listener, -1) {
            /** {@inheritDoc} */
            @Override
            public boolean equals(Object obj) {
                return listener == ((Container<L>) obj).listener;
            }

            @Override
            public int hashCode() {
                return listener.hashCode();
            }
        });
    }

    /**
     * Returns an iterator of the listeners for the {@code notificationNumber} (were added for and before it).
     *
     * <p>NOTE: {@link Iterator#remove} - not supported.
     *
     * <p>TODO: https://issues.apache.org/jira/browse/IGNITE-26175
     *
     * @param notificationNumber Configuration notification listener number.
     * @see ConfigurationListenerHolder#addListener
     */
    @SuppressWarnings("PMD.UseDiamondOperator")
    public Iterator<L> listeners(long notificationNumber) {
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

                    if (next.notificationNumber <= notificationNumber) {
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

        final long notificationNumber;

        Container(L listener, long storageRevision) {
            this.listener = listener;
            this.notificationNumber = storageRevision;
        }
    }
}
