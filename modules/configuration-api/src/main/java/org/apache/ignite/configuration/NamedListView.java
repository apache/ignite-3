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

import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.jetbrains.annotations.Nullable;

/**
 * View type for a {@link NamedConfigurationTree}. Represents an immutable snapshot of a named list configuration.
 *
 * @param <VIEWT> Type for immutable snapshots of named list elements.
 */
public interface NamedListView<VIEWT> extends Iterable<VIEWT> {
    /**
     * Returns an immutable collection of keys contained within this list.
     *
     * @return Immutable collection of keys contained within this list.
     */
    List<String> namedListKeys();

    /**
     * Returns value associated with the passed key.
     *
     * @param key Key string.
     * @return Requested value or {@code null} if it's not found.
     */
    @Nullable
    VIEWT get(String key);

    /**
     * Returns value associated with the passed internal id.
     *
     * @param internalId Internal id.
     * @return Requested value or {@code null} if it's not found.
     */
    @Nullable
    VIEWT get(UUID internalId);

    /**
     * Returns value located at the specified index.
     *
     * @param index Value index.
     * @return Requested value.
     * @throws IndexOutOfBoundsException If index is out of bounds.
     */
    VIEWT get(int index) throws IndexOutOfBoundsException;

    /**
     * Returns the number of elements in this list.
     *
     * @return Number of elements.
     */
    int size();

    /**
     * Returns the true if this list is empty.
     *
     * @return true if this list is empty.
     */
    default boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Returns an ordered stream of values from the named list.
     */
    Stream<VIEWT> stream();

    @Override
    default Iterator<VIEWT> iterator() {
        return stream().iterator();
    }
}
