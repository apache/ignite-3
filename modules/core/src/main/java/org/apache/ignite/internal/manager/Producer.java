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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiPredicate;

/**
 * Interface which can to produce own event.
 */
public abstract class Producer<T extends Event> {
    /** All listeners. */
    private ConcurrentHashMap<T, ConcurrentLinkedQueue<BiPredicate<List<Object>, Exception>>> listeners = new ConcurrentHashMap<>();

    /**
     * Registers an event listener.
     * When the event predicate return true it would never invoke after,
     * otherwise this predicate would receive an event again.
     *
     * @param evt Event.
     * @param closure Closure.
     */
    public void listen(T evt, BiPredicate<List<Object>, Exception> closure) {
        listeners.computeIfAbsent(evt, evtKey -> new ConcurrentLinkedQueue<>())
            .offer(closure);
    }

    /**
     * Notifies every listener with subscribed before.
     *
     * @param evt Event type.
     * @param params Event parameters.
     * @param err Exception when it was happened, or {@code null} otherwise.
     */
    protected void onEvent(T evt, List<Object> params, Exception err) {
        ConcurrentLinkedQueue<BiPredicate<List<Object>, Exception>> queue = listeners.get(evt);

        if (queue == null)
            return;

        BiPredicate<List<Object>, Exception> closure;

        Iterator<BiPredicate<List<Object>, Exception>> iter = queue.iterator();

        while (iter.hasNext()) {
            closure = iter.next();

            if (closure.test(params, err))
                iter.remove();
        }
    }
}
