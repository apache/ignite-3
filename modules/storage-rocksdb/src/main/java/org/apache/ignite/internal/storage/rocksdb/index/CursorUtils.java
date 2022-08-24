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

package org.apache.ignite.internal.storage.rocksdb.index;

import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class for working with cursors.
 */
class CursorUtils {
    /**
     * Cursor wrapper that discards elements while they match a given predicate. As soon as any element does not match the predicate,
     * no more elements will be discarded.
     *
     * @param <T> Cursor element type.
     */
    private static class DropWhileCursor<T> implements Cursor<T> {
        private final Cursor<T> cursor;

        @Nullable
        private Predicate<T> predicate;

        @Nullable
        private T skippedElement;

        DropWhileCursor(Cursor<T> cursor, Predicate<T> predicate) {
            this.cursor = cursor;
            this.predicate = predicate;
        }

        @Override
        public void close() throws Exception {
            cursor.close();
        }

        @Override
        public boolean hasNext() {
            if (predicate == null) {
                return skippedElement != null || cursor.hasNext();
            }

            while (cursor.hasNext()) {
                skippedElement = cursor.next();

                if (!predicate.test(skippedElement)) {
                    predicate = null;

                    break;
                }
            }

            return predicate == null;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            if (skippedElement != null) {
                T next = skippedElement;

                skippedElement = null;

                return next;
            } else {
                return cursor.next();
            }
        }
    }

    /**
     * Creates a cursor wrapper that discards elements while they match a given predicate. As soon as any element does not match the
     * predicate, no more elements will be discarded.
     *
     * @param cursor Underlying cursor with data.
     * @param predicate Predicate for elements to be discarded.
     * @param <T> Cursor element type.
     * @return Cursor wrapper.
     */
    static <T> Cursor<T> dropWhile(Cursor<T> cursor, Predicate<T> predicate) {
        return new DropWhileCursor<>(cursor, predicate);
    }

    /**
     * Cursor wrapper that discards elements if they don't match a given predicate. As soon as any element does not match the predicate,
     * all following elements will be discarded.
     *
     * @param <T> Cursor element type.
     */
    private static class TakeWhileCursor<T> implements Cursor<T> {
        private final Cursor<T> cursor;

        @Nullable
        private Predicate<T> predicate;

        @Nullable
        private T next;

        TakeWhileCursor(Cursor<T> cursor, Predicate<T> predicate) {
            this.cursor = cursor;
            this.predicate = predicate;
        }

        @Override
        public void close() throws Exception {
            cursor.close();
        }

        @Override
        public boolean hasNext() {
            if (next != null) {
                return true;
            } else if (predicate == null || !cursor.hasNext()) {
                return false;
            }

            next = cursor.next();

            if (predicate.test(next)) {
                return true;
            } else {
                predicate = null;
                next = null;

                return false;
            }
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            T result = next;

            next = null;

            return result;
        }
    }

    /**
     * Creates a cursor wrapper that discards elements if they don't match a given predicate. As soon as any element does not match the
     * predicate, all following elements will be discarded.
     *
     * @param cursor Underlying cursor with data.
     * @param predicate Predicate for elements to be kept.
     * @param <T> Cursor element type.
     * @return Cursor wrapper.
     */
    static <T> Cursor<T> takeWhile(Cursor<T> cursor, Predicate<T> predicate) {
        return new TakeWhileCursor<>(cursor, predicate);
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
        public void close() throws Exception {
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
    static <T, U> Cursor<U> map(Cursor<T> cursor, Function<T, U> mapper) {
        return new MapCursor<>(cursor, mapper);
    }

    /**
     * Creates a cursor that iterates over both given cursors.
     *
     * @param a First cursor.
     * @param b Second cursor.
     * @param <T> Cursor element type.
     * @return Cursor that iterates over both given cursors.
     */
    static <T> Cursor<T> concat(Cursor<T> a, Cursor<T> b) {
        return new Cursor<>() {
            private Cursor<T> currentCursor = a;

            @Override
            public void close() throws Exception {
                IgniteUtils.closeAll(a, b);
            }

            @Override
            public boolean hasNext() {
                if (currentCursor.hasNext()) {
                    return true;
                } else if (currentCursor == b) {
                    return false;
                } else {
                    currentCursor = b;

                    return currentCursor.hasNext();
                }
            }

            @Override
            public T next() {
                return currentCursor.next();
            }
        };
    }
}
