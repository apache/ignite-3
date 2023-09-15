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

/** Allows to {@link #listen add} and {@link #removeListener remove} event listeners that the component will fire. */
public interface EventProducer<T extends Event, P extends EventParameters> {
    /**
     * Registers an event listener. When the event predicate returns true it would never invoke after, otherwise this predicate would
     * receive an event again.
     *
     * @param evt Event.
     * @param listener Listener.
     */
    void listen(T evt, EventListener<? extends P> listener);

    /**
     * Removes a listener associated with the event.
     *
     * @param evt Event.
     * @param listener Listener.
     */
    void removeListener(T evt, EventListener<? extends P> listener);
}
