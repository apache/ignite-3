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

package org.apache.ignite.internal.compute.queue;

/**
 * Queue listener.
 *
 * @param <E> Queue element type.
 */
public interface QueueListener<E> {
    /**
     * Empty queue listener.
     *
     * @param <T> Queue element type.
     * @return Instance of empty queue listener.
     */
    static <T> QueueListener<T> noop() {
        return new QueueListener<>() {
            @Override
            public void onAdd(T t) {

            }

            @Override
            public void onTake(T t) {

            }
        };
    }

    /**
     * Calls when element added to queue.
     *
     * @param e Queue element which added to queue.
     */
    void onAdd(E e);

    /**
     * Calls when element taken from queue.
     *
     * @param e Queue element which taken from queue.
     */
    void onTake(E e);
}
