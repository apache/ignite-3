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

package org.apache.ignite.internal.manager;

import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Interface which can produce its events.
 */
public interface Producer<T extends Event, P extends EventParameters> {
    /**
     * Registers an event listener. When the event predicate returns true it would never invoke after, otherwise this predicate would
     * receive an event again.
     *
     * @param evt     Event.
     * @param closure Closure.
     */
    void listen(T evt, EventListener<P> closure);

    /**
     * Removes a listener associated with the event.
     *
     * @param evt     Event.
     * @param closure Closure.
     */
    void removeListener(T evt, EventListener<P> closure);

    /**
     * Removes a listener associated with the event.
     *
     * @param evt     Event.
     * @param closure Closure.
     * @param cause   The exception that was a cause which a listener is removed.
     */
    void removeListener(T evt, EventListener<P> closure, @Nullable IgniteInternalCheckedException cause);
}
