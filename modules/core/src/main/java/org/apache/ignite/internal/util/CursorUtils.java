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

package org.apache.ignite.internal.util;

import java.util.Collections;
import java.util.function.Function;

/**
 * Utility class for working with cursors.
 */
public class CursorUtils {
    /** Empty cursor instance. */
    private static final Cursor<?> EMPTY = Cursor.fromBareIterator(Collections.emptyIterator());

    /**
     * Creates an empty cursor.
     *
     * @param <T> Type of elements in iterator.
     * @return Cursor.
     */
    public static <T> Cursor<T> emptyCursor() {
        return (Cursor<T>) EMPTY;
    }

    /**
     * Cursor wrapper that transforms the underlying cursor's data using the provided mapping function.
     *
     * @param <T> Type of the original data.
     * @param <U> Type of the transformed data.
     */
    private static class MapCursor<T, U> implements Cursor<U> {
        private final Cursor<T> cursor;

        private final Function<T, U> mapper;

        MapCursor(Cursor<T> cursor, Function<T, U> mapper) {
            this.cursor = cursor;
            this.mapper = mapper;
        }

        @Override
        public void close() {
            cursor.close();
        }

        @Override
        public boolean hasNext() {
            return cursor.hasNext();
        }

        @Override
        public U next() {
            return mapper.apply(cursor.next());
        }
    }

    /**
     * Creates a cursor wrapper that transforms the underlying cursor's data using the provided mapping function.
     *
     * @param cursor Underlying cursor with data.
     * @param mapper Function to transform the elements of the underlying cursor.
     * @param <T> Type of the original data.
     * @param <U> Type of the transformed data.
     * @return Cursor wrapper.
     */
    public static <T, U> Cursor<U> map(Cursor<T> cursor, Function<T, U> mapper) {
        return new MapCursor<>(cursor, mapper);
    }
}
