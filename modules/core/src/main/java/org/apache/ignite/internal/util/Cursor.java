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

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
     * Creates an iterator based cursor.
     *
     * @param it Iterator.
     * @param <T> Type of elements in iterator.
     * @return Cursor.
     */
    static <T> Cursor<T> fromIterator(Iterator<? extends T> it) {
        return new Cursor<T>() {
            /** {@inheritDoc} */
            @Override
            public void close() throws Exception {
                if (it instanceof AutoCloseable) {
                    ((AutoCloseable) it).close();
                }
            }

            /** {@inheritDoc} */
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            /** {@inheritDoc} */
            @Override
            public T next() {
                return it.next();
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
}
