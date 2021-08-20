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

package org.apache.ignite.internal.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.addAll;
import static java.util.Collections.unmodifiableSet;

/**
 * Utility class provides various method to work with collections.
 */
public final class CollectionUtils {
    /**
     * Tests if the given collection is either {@code null} or empty.
     *
     * @param col Collection to check.
     * @return Whether or not the given collection is {@code null} or empty.
     */
    public static boolean nullOrEmpty(@Nullable Collection<?> col) {
        return col == null || col.isEmpty();
    }

    /**
     * Tests if the given collection is either {@code null} or empty.
     *
     * @param c Collection to test.
     * @return Whether or not the given collection is {@code null} or empty.
     */
    public static boolean nullOrEmpty(@Nullable Iterable<?> c) {
        return c == null || (c instanceof Collection<?> ? ((Collection<?>) c).isEmpty() : !c.iterator().hasNext());
    }

    /**
     * Tests if the given collection is either {@code null} or empty.
     *
     * @param col Collection to check.
     * @return Whether or not the given collection is {@code null} or empty.
     */
    public static boolean nullOrEmpty(@Nullable Map<?, ?> col) {
        return col == null || col.isEmpty();
    }

    /**
     * Gets first element from given list or returns {@code null} if list is empty.
     *
     * @param list List to retrieve the first element.
     * @param <T> Type of the elements of the list.
     * @return The first element of the given list or {@code null} in case the list is empty.
     */
    public static <T> T first(List<? extends T> list) {
        if (nullOrEmpty(list))
            return null;

        return list.get(0);
    }

    /**
     * Union set and items.
     *
     * @param set Set.
     * @param ts Items.
     * @param <T> Type of the elements of the list.
     * @return Immutable union of set and items.
     */
    @SafeVarargs
    public static <T> Set<T> union(@Nullable Set<T> set, @Nullable T... ts) {
        if (nullOrEmpty(set))
            return ts == null || ts.length == 0 ? Set.of() : Set.of(ts);
        else if (ts == null || ts.length == 0)
            return unmodifiableSet(set);
        else {
            Set<T> res = new HashSet<>(set);

            addAll(res, ts);

            return unmodifiableSet(res);
        }
    }

    /** Stub. */
    private CollectionUtils() {
        // No op.
    }
}
