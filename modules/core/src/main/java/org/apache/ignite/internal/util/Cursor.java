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

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.ignite.internal.lang.IgniteInternalException;

/**
 * Closeable cursor.
 *
 * @param <T> Type of elements.
 */
public interface Cursor<T> extends Iterator<T>, Iterable<T>, AutoCloseable {
    /** {@inheritDoc} */
    @Override
    default Iterator<T> iterator() {
        return this;
    }

    /**
     * Creates a cursor iterating over data provided by a bare iterator. A bare iterator is an {@link Iterator}
     * that does not implement {@link AutoCloseable} and hence does not need to be closed.
     *
     * <p>If a closeable iterator needs to be adapted, {@link #fromCloseableIterator(Iterator)} should be used instead.
     *
     * @param it Iterator.
     * @param <T> Type of elements in iterator.
     * @return Cursor.
     */
    static <T> Cursor<T> fromBareIterator(Iterator<? extends T> it) {
        if (it instanceof AutoCloseable) {
            throw new IllegalArgumentException(it.getClass().getName() + " implements AutoCloseable, while Cursor#fromBareIterator() only "
                    + "supports non-closeable iterators. Please refer to Cursor#fromCloseableIterator().");
        }

        return new IteratorCursor<>(it) {
            @Override
            public void close() {
                // No-op.
            }
        };
    }

    /**
     * Creates an iterable based cursor.
     *
     * @param iterable Iterable.
     * @param <T> Type of elements.
     * @return Cursor.
     */
    static <T> Cursor<T> fromIterable(Iterable<? extends T> iterable) {
        return fromBareIterator(iterable.iterator());
    }

    /**
     * Creates a cursor adapting a closeable iterator. The iterator will be closed when the resulting cursor is closed.
     *
     * <p>Please DO NOT use this method without a careful consideration. It handles checked exceptions happening inside
     * {@code close()} by rethrowing them wrapped in a runtime exception, which should generally be avoided; instead, it
     * is recommended to write a specific Cursor implementation that handles the checked exceptions accordingly to its
     * knowledge about the nature of the data source providing the iterator.
     *
     * @param iterator Iterator to adapt.
     * @return Cursor.
     */
    static <T, I extends Iterator<? extends T> & AutoCloseable> Cursor<T> fromCloseableIterator(I iterator) {
        return new IteratorCursor<>(iterator) {
            @Override
            public void close() {
                try {
                    iterator.close();
                } catch (Exception e) {
                    throw new IgniteInternalException(e);
                }
            }
        };
    }

    /**
     * Returns a sequential Stream over the elements covered by this cursor.
     *
     * @return Sequential Stream over the elements covered by this cursor.
     */
    default Stream<T> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    /**
     * Closes the Cursor also releasing all underlying resources.
     *
     * <p>This method should be implemented carefully; just wrapping a checked exception in an unchecked one
     * should not be used as the default option.
     */
    @Override
    void close();
}
