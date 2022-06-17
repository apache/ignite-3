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

import static java.util.Collections.emptyIterator;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.util.CollectionUtils.concat;
import static org.apache.ignite.internal.util.CollectionUtils.difference;
import static org.apache.ignite.internal.util.CollectionUtils.setOf;
import static org.apache.ignite.internal.util.CollectionUtils.union;
import static org.apache.ignite.internal.util.CollectionUtils.viewReadOnly;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterators;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;

/**
 * Testing the {@link CollectionUtils}.
 */
public class CollectionUtilsTest {
    @Test
    void testConcatIterables() {
        assertTrue(collect(concat((Iterable<?>[]) null)).isEmpty());
        assertTrue(collect(concat((Iterable<?>) List.of())).isEmpty());
        assertTrue(collect(concat(List.of(), List.of())).isEmpty());

        assertEquals(List.of(1), collect(concat(List.of(1))));
        assertEquals(List.of(1), collect(concat(List.of(1), List.of())));
        assertEquals(List.of(1), collect(concat(List.of(), List.of(1))));

        assertEquals(List.of(1, 2, 3), collect(concat(List.of(1), List.of(2, 3))));
    }

    @Test
    void testSetUnion() {
        assertTrue(union(null, null).isEmpty());
        assertTrue(union(Set.of(), null).isEmpty());
        assertTrue(union(null, new Object[]{}).isEmpty());

        assertEquals(Set.of(1), union(Set.of(1), null));
        assertEquals(Set.of(1), union(Set.of(1), new Integer[]{}));
        assertEquals(Set.of(1), union(null, 1));
        assertEquals(Set.of(1), union(Set.of(), 1));

        assertEquals(Set.of(1, 2), union(Set.of(1), 2));
    }

    @Test
    void testViewReadOnly() {
        assertTrue(viewReadOnly(null, null).isEmpty());
        assertTrue(viewReadOnly(List.of(), null).isEmpty());

        assertEquals(List.of(1), collect(viewReadOnly(List.of(1), null)));
        assertEquals(List.of(1), collect(viewReadOnly(List.of(1), identity())));

        assertEquals(List.of("1", "2", "3"), collect(viewReadOnly(List.of(1, 2, 3), String::valueOf)));

        assertThrows(UnsupportedOperationException.class, () -> viewReadOnly(List.of(1), null).add(1));
        assertThrows(UnsupportedOperationException.class, () -> viewReadOnly(List.of(1), null).addAll(List.of()));

        assertThrows(UnsupportedOperationException.class, () -> viewReadOnly(List.of(1), null).remove(1));
        assertThrows(UnsupportedOperationException.class, () -> viewReadOnly(List.of(1), null).removeAll(List.of()));
        assertThrows(UnsupportedOperationException.class, () -> viewReadOnly(List.of(1), null).removeIf(o -> true));
        assertThrows(UnsupportedOperationException.class, () -> viewReadOnly(List.of(1), null).clear());

        assertThrows(UnsupportedOperationException.class, () -> viewReadOnly(List.of(1), null).retainAll(List.of()));

        assertThrows(UnsupportedOperationException.class, () -> viewReadOnly(List.of(1), null).iterator().remove());
    }

    @Test
    void testViewReadOnlyWithPredicate() {
        assertTrue(viewReadOnly(null, null, null).isEmpty());
        assertTrue(viewReadOnly(List.of(), null, null).isEmpty());

        assertEquals(List.of(1), collect(viewReadOnly(List.of(1), null, null)));
        assertEquals(List.of(1), collect(viewReadOnly(List.of(1), identity(), null)));
        assertEquals(List.of(1), collect(viewReadOnly(List.of(1), null, integer -> true)));
        assertEquals(List.of(1), collect(viewReadOnly(List.of(1), identity(), integer -> true)));
        assertEquals(List.of(), collect(viewReadOnly(List.of(1), null, integer -> false)));
        assertEquals(List.of(), collect(viewReadOnly(List.of(1), identity(), integer -> false)));

        assertEquals(List.of("1", "2", "3"), collect(viewReadOnly(List.of(1, 2, 3), String::valueOf, null)));
        assertEquals(List.of("3"), collect(viewReadOnly(List.of(1, 2, 3), String::valueOf, integer -> integer > 2)));

        assertEquals(4, viewReadOnly(List.of(1, 2, 3, 4), String::valueOf, integer -> true).size());
        assertEquals(4, viewReadOnly(List.of(1, 2, 3, 4), String::valueOf, null).size());
        assertEquals(2, viewReadOnly(List.of(1, 2, 3, 4), identity(), integer -> integer < 3).size());
        assertEquals(0, viewReadOnly(List.of(1, 2, 3, 4), identity(), integer -> false).size());

        assertFalse(viewReadOnly(List.of(1, 2, 3, 4), String::valueOf, integer -> true).isEmpty());
        assertFalse(viewReadOnly(List.of(1, 2, 3, 4), String::valueOf, null).isEmpty());
        assertFalse(viewReadOnly(List.of(1, 2, 3, 4), identity(), integer -> integer > 3).isEmpty());
        assertTrue(viewReadOnly(List.of(1, 2, 3, 4), identity(), integer -> integer > 4).isEmpty());
        assertTrue(viewReadOnly(List.of(1, 2, 3, 4), identity(), integer -> false).isEmpty());

        assertDoesNotThrow(() -> viewReadOnly(Arrays.asList(new Integer[]{null}), null, null).iterator().next());
        assertDoesNotThrow(() -> viewReadOnly(Arrays.asList(new Integer[]{null}), null, integer -> true).iterator().next());
        assertDoesNotThrow(() -> viewReadOnly(Arrays.asList(null, 1), null, null).iterator().next());
        assertDoesNotThrow(() -> viewReadOnly(Arrays.asList(null, 1), null, integer -> true).iterator().next());

        assertThrows(
                NoSuchElementException.class,
                () -> viewReadOnly(Arrays.asList(new Integer[]{null}), null, integer -> false).iterator().next()
        );

        assertThrows(
                NoSuchElementException.class,
                () -> {
                    Iterator<Object> iterator = viewReadOnly(Arrays.asList(null, 1), null, Objects::nonNull).iterator();
                    iterator.next();
                    iterator.next();
                }
        );

        assertThrows(UnsupportedOperationException.class, () -> viewReadOnly(List.of(1), null, null).add(1));
        assertThrows(UnsupportedOperationException.class, () -> viewReadOnly(List.of(1), null, null).addAll(List.of()));

        assertThrows(UnsupportedOperationException.class, () -> viewReadOnly(List.of(1), null, null).remove(1));
        assertThrows(UnsupportedOperationException.class, () -> viewReadOnly(List.of(1), null, null).removeAll(List.of()));
        assertThrows(UnsupportedOperationException.class, () -> viewReadOnly(List.of(1), null, null).removeIf(o -> true));
        assertThrows(UnsupportedOperationException.class, () -> viewReadOnly(List.of(1), null, null).clear());

        assertThrows(UnsupportedOperationException.class, () -> viewReadOnly(List.of(1), null, null).retainAll(List.of()));

        assertThrows(UnsupportedOperationException.class, () -> viewReadOnly(List.of(1), null, null).iterator().remove());
    }

    @Test
    void testSetDifference() {
        assertTrue(difference(null, Set.of(1, 2, 3, 4)).isEmpty());
        assertTrue(difference(Set.of(), Set.of(1, 2, 3, 4)).isEmpty());

        assertEquals(Set.of(1, 2, 3, 4), difference(Set.of(1, 2, 3, 4), null));
        assertEquals(Set.of(1, 2, 3, 4), difference(Set.of(1, 2, 3, 4), Set.of()));

        assertEquals(Set.of(1, 2), difference(Set.of(1, 2, 3, 4), Set.of(3, 4)));
        assertEquals(Set.of(1, 4), difference(Set.of(1, 2, 3, 4), Set.of(2, 3)));

        assertEquals(Set.of(1, 2, 3, 4), difference(Set.of(1, 2, 3, 4), Set.of(5, 6, 7)));

        assertEquals(Set.of(), difference(Set.of(1, 2, 3, 4), Set.of(1, 2, 3, 4)));
    }

    @Test
    void testCollectionUnion() {
        assertTrue(union(new Collection[0]).isEmpty());
        assertTrue(union((Collection<Object>[]) null).isEmpty());
        assertTrue(union((Collection<Object>) List.of()).isEmpty());

        assertEquals(List.of(1), collect(union(List.of(1), Set.of())));
        assertEquals(List.of(1), collect(union(List.of(), Set.of(1))));

        assertEquals(List.of(1, 2), collect(union(List.of(1), Set.of(2))));
        assertEquals(List.of(1, 2, 2), collect(union(List.of(1), List.of(2), Set.of(2))));

        assertFalse(union(new Collection[0]).contains(0));
        assertFalse(union((Collection<Object>) List.of()).contains(0));
        assertFalse(union((Collection<Integer>) List.of(1)).contains(0));
        assertFalse(union(List.of(1), Set.of()).contains(0));
        assertFalse(union(List.of(), Set.of(1)).contains(0));
        assertFalse(union(List.of(1), Set.of(2, 3)).contains(0));

        assertTrue(union((Collection<Integer>) List.of(0)).contains(0));
        assertTrue(union(List.of(), Set.of(0)).contains(0));
        assertTrue(union(List.of(0), Set.of()).contains(0));

        assertEquals(0, union(new Collection[0]).size());
        assertEquals(0, union((Collection<Object>) List.of()).size());
        assertEquals(1, union((Collection<Integer>) List.of(1)).size());
        assertEquals(1, union(List.of(), Set.of(1)).size());
        assertEquals(1, union(List.of(1), Set.of()).size());
        assertEquals(2, union(List.of(1), Set.of(2)).size());
        assertEquals(3, union(List.of(1), Set.of(2, 3)).size());
        assertEquals(5, union(List.of(1, 4, 5), Set.of(2, 3)).size());

        Collection<Integer> integers = new ArrayList<>(List.of(1, 2, 3));

        Collection<Integer> union = union(integers);

        integers.remove(1);

        assertEquals(2, union.size());
    }

    /**
     * Test setOf by populated and empty list.
     */
    @Test
    void testSetOfList() {
        List<Integer> list = Arrays.asList(1, 2, 3, 234, 3);

        testSetOf(list);

        testSetOf(Collections.emptyList());
    }

    /**
     * Test setOf by populated and empty sets.
     */
    @Test
    void testSetOfSet() {
        Set<Integer> set = new HashSet<>();
        set.add(1);
        set.add(2);
        set.add(3);
        set.add(234);

        testSetOf(set);

        testSetOf(Collections.emptySet());
    }

    @Test
    void testConcatIterators() {
        assertTrue(collect(concat((Iterator<?>[]) null)).isEmpty());
        assertTrue(collect(concat(emptyIterator())).isEmpty());
        assertTrue(collect(concat(emptyIterator(), emptyIterator())).isEmpty());

        assertEquals(List.of(1), collect(concat(List.of(1).iterator())));
        assertEquals(List.of(1), collect(concat(List.of(1).iterator(), emptyIterator())));
        assertEquals(List.of(1), collect(concat(emptyIterator(), List.of(1).iterator())));

        assertEquals(List.of(1, 2, 3), collect(concat(List.of(1).iterator(), List.of(2, 3).iterator())));
    }

    @Test
    void testConcatCollectionOfIterators() {
        assertTrue(collect(concat((Collection<Iterator<?>>) null)).isEmpty());
        assertTrue(collect(concat(List.of())).isEmpty());
        assertTrue(collect(concat(List.of(emptyIterator(), emptyIterator()))).isEmpty());

        assertEquals(List.of(1), collect(concat(List.of(List.of(1).iterator()))));
        assertEquals(List.of(1), collect(concat(List.of(List.of(1).iterator(), emptyIterator()))));
        assertEquals(List.of(1), collect(concat(List.of(emptyIterator(), List.of(1).iterator()))));

        assertEquals(List.of(1, 2, 3), collect(concat(List.of(List.of(1).iterator(), List.of(2, 3).iterator()))));
    }

    @Test
    void testListUnion() {
        assertTrue(union(new List[0]).isEmpty());
        assertTrue(union((List<Object>[]) null).isEmpty());
        assertTrue(union(List.of()).isEmpty());

        assertEquals(List.of(1), collect(union(List.of(1), List.of())));
        assertEquals(List.of(1), collect(union(List.of(), List.of(1))));

        assertEquals(List.of(1, 2), collect(union(List.of(1), List.of(2))));
        assertEquals(List.of(1, 2, 2), collect(union(List.of(1), List.of(2), List.of(2))));

        assertFalse(union(new List[0]).contains(0));
        assertFalse(union(List.of()).contains(0));
        assertFalse(union(List.of(1)).contains(0));
        assertFalse(union(List.of(1), List.of()).contains(0));
        assertFalse(union(List.of(), List.of(1)).contains(0));
        assertFalse(union(List.of(1), List.of(2, 3)).contains(0));

        assertTrue(union(List.of(0)).contains(0));
        assertTrue(union(List.of(), List.of(0)).contains(0));
        assertTrue(union(List.of(0), List.of()).contains(0));

        assertEquals(0, union(new List[0]).size());
        assertEquals(0, union(List.of()).size());
        assertEquals(1, union(List.of(1)).size());
        assertEquals(1, union(List.of(), List.of(1)).size());
        assertEquals(1, union(List.of(1), List.of()).size());
        assertEquals(2, union(List.of(1), List.of(2)).size());
        assertEquals(3, union(List.of(1), List.of(2, 3)).size());
        assertEquals(5, union(List.of(1, 4, 5), List.of(2, 3)).size());

        List<Integer> integers = new ArrayList<>(List.of(1, 2, 3));

        List<Integer> union0 = union(integers);

        integers.remove(1);

        assertEquals(2, union0.size());

        List<Integer> union1 = union(List.of(0), List.of(1, 2), List.of(3, 4, 5));

        assertEquals(0, union1.get(0));
        assertEquals(1, union1.get(1));
        assertEquals(2, union1.get(2));
        assertEquals(3, union1.get(3));
        assertEquals(4, union1.get(4));
        assertEquals(5, union1.get(5));

        assertThrows(IndexOutOfBoundsException.class, () -> union1.get(6));
    }

    /**
     * Collect of elements.
     *
     * @param iterable Iterable.
     * @param <T> Type of the elements.
     * @return Collected elements.
     */
    private <T> List<? extends T> collect(Iterable<? extends T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false).collect(toList());
    }

    /**
     * Collect of elements.
     *
     * @param iterator Iterator.
     * @param <T> Type of the elements.
     * @return Collected elements.
     */
    private <T> List<? extends T> collect(Iterator<? extends T> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false).collect(toList());
    }

    private void testSetOf(Collection<Integer> data) {
        Set<Integer> copy = setOf(data);

        assertNotNull(copy);

        for (Integer i : data) {
            assertTrue(copy.contains(i));
        }

        assertEquals(new HashSet<>(data).size(), copy.size());

        assertThrows(UnsupportedOperationException.class, () -> copy.add(42));
        assertThrows(UnsupportedOperationException.class, () -> copy.remove(3));
    }
}
