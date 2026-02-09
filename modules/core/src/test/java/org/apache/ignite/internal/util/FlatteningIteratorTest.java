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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

/** Set of tests to verify {@link FlatteningIterator}. */
@SuppressWarnings("resource")
class FlatteningIteratorTest {
    @Test
    void testBasicFlattening() {
        List<List<Integer>> input = List.of(
                List.of(1, 2, 3),
                List.of(4, 5),
                List.of(6, 7, 8, 9)
        );

        FlatteningIterator<Integer> iterator = new FlatteningIterator<>(input.iterator());

        List<Integer> result = new ArrayList<>();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }

        assertEquals(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9), result);
    }

    @Test
    void testEmptySource() {
        List<List<Integer>> input = List.of();

        @SuppressWarnings("RedundantOperationOnEmptyContainer")
        FlatteningIterator<Integer> iterator = new FlatteningIterator<>(input.iterator());

        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    void testAllEmptyIterables() {
        List<List<Integer>> input = List.of(
                List.of(),
                List.of(),
                List.of()
        );

        FlatteningIterator<Integer> iterator = new FlatteningIterator<>(input.iterator());

        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    void testMixedEmptyAndNonEmpty() {
        List<List<Integer>> input = List.of(
                List.of(),
                List.of(1, 2),
                List.of(),
                List.of(3),
                List.of()
        );

        FlatteningIterator<Integer> iterator = new FlatteningIterator<>(input.iterator());

        List<Integer> result = new ArrayList<>();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }

        assertEquals(List.of(1, 2, 3), result);
    }

    @Test
    void testSingleElementSingleIterable() {
        List<List<String>> input = List.of(
                List.of("hello")
        );

        FlatteningIterator<String> iterator = new FlatteningIterator<>(input.iterator());

        assertTrue(iterator.hasNext());
        assertEquals("hello", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    void testExhaustedIterator() {
        List<List<Integer>> input = List.of(List.of(1, 2));

        FlatteningIterator<Integer> iterator = new FlatteningIterator<>(input.iterator());

        iterator.next(); // 1
        iterator.next(); // 2

        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    void testMultipleHasNextCalls() {
        List<List<Integer>> input = List.of(List.of(1, 2));

        FlatteningIterator<Integer> iterator = new FlatteningIterator<>(input.iterator());

        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasNext());

        assertEquals(1, iterator.next());

        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasNext());

        assertEquals(2, iterator.next());
    }

    @Test
    void testWithStrings() {
        List<List<String>> input = List.of(
                List.of("a", "b"),
                List.of("c", "d", "e")
        );

        FlatteningIterator<String> iterator = new FlatteningIterator<>(input.iterator());

        List<String> result = new ArrayList<>();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }

        assertEquals(List.of("a", "b", "c", "d", "e"), result);
    }

    @Test
    void testAutoCloseable() {
        class ClosableIterator<T> implements Iterator<T>, AutoCloseable {
            private final Iterator<T> delegate;
            private boolean closeCalled;

            private ClosableIterator(Iterator<T> delegate) {
                this.delegate = delegate;
            }

            @Override
            public void close() {
                closeCalled = true;
            }

            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public T next() {
                return delegate.next();
            }
        }

        ClosableIterator<List<Integer>> input = new ClosableIterator<>(List.of(
                List.of(1, 2, 3)
        ).iterator());

        assertDoesNotThrow(() -> {
            try (FlatteningIterator<Integer> iterator = new FlatteningIterator<>(input)) {
                iterator.next();
            }
        });
        assertTrue(input.closeCalled);
    }

    @Test
    void testEmptyIterableAtBeginning() {
        List<List<Integer>> input = List.of(
                List.of(),
                List.of(1, 2, 3)
        );

        FlatteningIterator<Integer> iterator = new FlatteningIterator<>(input.iterator());

        assertTrue(iterator.hasNext());
        assertEquals(1, iterator.next());
    }

    @Test
    void testEmptyIterableAtEnd() {
        List<List<Integer>> input = List.of(
                List.of(1, 2, 3),
                List.of()
        );

        FlatteningIterator<Integer> iterator = new FlatteningIterator<>(input.iterator());

        iterator.next(); // 1
        iterator.next(); // 2
        iterator.next(); // 3

        assertFalse(iterator.hasNext());
    }
}
