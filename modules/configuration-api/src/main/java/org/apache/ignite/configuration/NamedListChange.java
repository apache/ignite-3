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

package org.apache.ignite.configuration;

import java.util.function.Consumer;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;

/**
 * Closure parameter for {@link NamedConfigurationTree#change(Consumer)} method. Contains methods to modify named lists.
 *
 * @param <VIEWT> Type for the reading named list elements of this particular list.
 * @param <CHANGET> Type for changing named list elements of this particular list.
 */
public interface NamedListChange<VIEWT, CHANGET extends VIEWT> extends NamedListView<VIEWT> {
    /**
     * Creates a new value in the named list configuration.
     *
     * @param key Key for the value to be created.
     * @param valConsumer Closure to modify the value associated with the key. Closure parameter must not be leaked
     *      outside the scope of the closure.
     * @return {@code this} for chaining.
     *
     * @throws NullPointerException If one of the parameters is null.
     * @throws IllegalArgumentException If an element with the given name already exists.
     */
    NamedListChange<VIEWT, CHANGET> create(String key, Consumer<CHANGET> valConsumer);

    /**
     * Creates a new value at the given position in the named list configuration.
     *
     * @param index Index of the inserted element.
     * @param key Key for the value to be created.
     * @param valConsumer Closure to modify the value associated with the key. Closure parameter must not be leaked
     *      outside the scope of the closure.
     * @return {@code this} for chaining.
     *
     * @throws NullPointerException If one of the parameters is null.
     * @throws IndexOutOfBoundsException If index is negative of exceeds the size of the list.
     * @throws IllegalArgumentException If an element with the given name already exists.
     */
    NamedListChange<VIEWT, CHANGET> create(int index, String key, Consumer<CHANGET> valConsumer);

    /**
     * Create a new value after a given precedingKey key in the named list configuration.
     *
     * @param precedingKey Name of the preceding element.
     * @param key Key for the value to be created.
     * @param valConsumer Closure to modify the value associated with the key. Closure parameter must not be leaked
     *      outside the scope of the closure.
     * @return {@code this} for chaining.
     *
     * @throws NullPointerException If one of parameters is null.
     * @throws IllegalArgumentException If element with given name already exists
     *      or if {@code precedingKey} element doesn't exist.
     */
    NamedListChange<VIEWT, CHANGET> createAfter(String precedingKey, String key, Consumer<CHANGET> valConsumer);

    /**
     * Updates a value in the named list configuration. If the value cannot be found, creates a new one instead.
     *
     * @param key Key for the value to be updated.
     * @param valConsumer Closure to modify the value associated with the key. Closure parameter must not be leaked
     *      outside the scope of the closure.
     * @return {@code this} for chaining.
     *
     * @throws NullPointerException If one of parameters is null.
     * @throws IllegalArgumentException If {@link #delete(String)} has been invoked with the same key previously.
     */
    NamedListChange<VIEWT, CHANGET> createOrUpdate(String key, Consumer<CHANGET> valConsumer);

    /**
     * Updates a value in the named list configuration.
     *
     * @param key Key for the value to be updated.
     * @param valConsumer Closure to modify the value associated with the key. Closure parameter must not be leaked
     *      outside the scope of the closure.
     * @return {@code this} for chaining.
     *
     * @throws NullPointerException If one of parameters is null.
     * @throws IllegalArgumentException If the given key does not exist or if {@link #delete(String)} has been invoked with the same key
     *                                  previously.
     */
    NamedListChange<VIEWT, CHANGET> update(String key, Consumer<CHANGET> valConsumer);

    /**
     * Renames the existing value in the named list configuration. Does nothing if {@code oldKey} and {@code newKey} are the same.
     * Element with key {@code oldKey} must exist and key {@code newKey} must not.
     * Error will occur if {@code newKey} has just been deleted on the same
     * {@link NamedListChange} instance (to distinguish between
     * {@link ConfigurationNamedListListener#onRename(ConfigurationNotificationEvent)} and
     * {@link ConfigurationNamedListListener#onUpdate(ConfigurationNotificationEvent)} on {@code newKey}).
     *
     * @param oldKey Key for the value to be updated.
     * @param newKey New key for the same value.
     * @return {@code this} for chaining.
     *
     * @throws NullPointerException If one of parameters is null.
     * @throws IllegalArgumentException If an element with name {@code newKey} already exists, or an element with name
     *      {@code oldKey} doesn't exist, or {@link #delete(String)} has previously been invoked with either the
     *      {@code newKey} or the {@code oldKey}.
     */
    NamedListChange<VIEWT, CHANGET> rename(String oldKey, String newKey);

    /**
     * Removes the value from the named list configuration.
     *
     * @param key Key for the value to be removed.
     * @return {@code this} for chaining.
     *
     * @throws NullPointerException If key is null.
     * @throws IllegalArgumentException If {@link #createOrUpdate(String, Consumer)} has been invoked with the same key
     *      previously.
     */
    NamedListChange<VIEWT, CHANGET> delete(String key);
}
