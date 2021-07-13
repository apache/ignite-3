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

package org.apache.ignite.configuration;

import java.util.function.Consumer;

/** */
public interface NamedListChange<Change> extends NamedListView<Change> {
    /**
     * Create new value in named list configuration.
     *
     * @param key Key for the value to be created.
     * @param valConsumer Closure to modify value associated with the key. Object of type {@code T},
     *      passed to the closure, must not be reused anywhere else.
     * @return {@code this} for chaining.
     *
     * @throws NullPointerException If one of parameters is null.
     * @throws IllegalArgumentException If element with given name already exists.
     */
    NamedListChange<Change> create(String key, Consumer<Change> valConsumer);

    /**
     * Create new value in named list configuration.
     *
     * @param index Elements insertion index.
     * @param key Key for the value to be created.
     * @param valConsumer Closure to modify value associated with the key. Object of type {@code T},
     *      passed to the closure, must not be reused anywhere else.
     * @return {@code this} for chaining.
     *
     * @throws NullPointerException If one of parameters is null.
     * @throws IndexOutOfBoundsException If index cannot be used for new element insertion.
     * @throws IllegalArgumentException If element with given name already exists.
     */
    NamedListChange<Change> create(int index, String key, Consumer<Change> valConsumer);

    /**
     * Create new value in named list configuration.
     *
     * @param base Name of th preceeding element.
     * @param key Key for the value to be created.
     * @param valConsumer Closure to modify value associated with the key. Object of type {@code T},
     *      passed to the closure, must not be reused anywhere else.
     * @return {@code this} for chaining.
     *
     * @throws NullPointerException If one of parameters is null.
     * @throws IllegalArgumentException If element with given name already exists
     *      or if {@code base} element doesn't exist.
     */
    NamedListChange<Change> createAfter(String base, String key, Consumer<Change> valConsumer);

    /**
     * Update the value in named list configuration. Create if it doesn't exist yet.
     *
     * @param key Key for the value to be updated.
     * @param valConsumer Closure to modify value associated with the key. Object of type {@code T},
     *      passed to the closure, must not be reused anywhere else.
     * @return {@code this} for chaining.
     *
     * @throws NullPointerException If one of parameters is null.
     * @throws IllegalArgumentException If {@link #delete(String)} has been invoked with the same key previously.
     */
    NamedListChange<Change> createOrUpdate(String key, Consumer<Change> valConsumer);

    /**
     * Remove the value from named list configuration.
     *
     * @param key Key for the value to be removed.
     * @return {@code this} for chaining.
     *
     * @throws NullPointerException If key is null.
     * @throws IllegalArgumentException If {@link #createOrUpdate(String, Consumer)} has been invoked with the same key previously.
     */
    NamedListChange<Change> delete(String key);
}
