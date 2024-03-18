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

import static java.util.Collections.emptyIterator;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.util.CollectionUtils.concat;
import static org.apache.ignite.internal.util.CollectionUtils.difference;
import static org.apache.ignite.internal.util.CollectionUtils.intersect;
import static org.apache.ignite.internal.util.CollectionUtils.last;
import static org.apache.ignite.internal.util.CollectionUtils.mapIterable;
import static org.apache.ignite.internal.util.CollectionUtils.setOf;
import static org.apache.ignite.internal.util.CollectionUtils.union;
import static org.apache.ignite.internal.util.CollectionUtils.view;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
    void testConcatCollection() {
        assertTrue(concat(new Collection[0]).isEmpty());
        assertTrue(concat((Collection<Object>[]) null).isEmpty());
        assertTrue(concat((Collection<Object>) List.of()).isEmpty());

        assertThat(concat(List.of(1), Set.of()), contains(1));
        assertThat(concat(List.of(), Set.of(1)), contains(1));

        assertThat(concat(List.of(1), Set.of(2)), contains(1, 2));
        assertThat(concat(List.of(1), List.of(2), Set.of(2)), contains(1, 2, 2));

        assertFalse(concat(new Collection[0]).contains(0));
        assertFalse(concat((Collection<Object>) List.of()).contains(0));
        assertFalse(concat((Collection<Integer>) List.of(1)).contains(0));
        assertFalse(concat(List.of(1), Set.of()).contains(0));
        assertFalse(concat(List.of(), Set.of(1)).contains(0));
        assertFalse(concat(List.of(1), Set.of(2, 3)).contains(0));

        assertTrue(concat((Collection<Integer>) List.of(0)).contains(0));
        assertTrue(concat(List.of(), Set.of(0)).contains(0));
        assertTrue(concat(List.of(0), Set.of()).contains(0));

        assertEquals(0, concat(new Collection[0]).size());
        assertEquals(0, concat((Collection<Object>) List.of()).size());
        assertEquals(1, concat((Collection<Integer>) List.of(1)).size());
        assertEquals(1, concat(List.of(), Set.of(1)).size());
        assertEquals(1, concat(List.of(1), Set.of()).size());
        assertEquals(2, concat(List.of(1), Set.of(2)).size());
        assertEquals(3, concat(List.of(1), Set.of(2, 3)).size());
        assertEquals(5, concat(List.of(1, 4, 5), Set.of(2, 3)).size());

        Collection<Integer> integers = new ArrayList<>(List.of(1, 2, 3));

        Collection<Integer> concat = concat(integers);

        integers.remove(1);

        assertEquals(2, concat.size());
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
    void testConcatIteratorOfIterators() {
        assertTrue(collect(concat((Collection<Iterator<?>>) null)).isEmpty());
        assertTrue(collect(concat(List.of())).isEmpty());
        assertTrue(collect(concat(List.of(emptyIterator(), emptyIterator()))).isEmpty());

        assertEquals(List.of(1), collect(concat(List.of(List.of(1).iterator()))));
        assertEquals(List.of(1), collect(concat(List.of(List.of(1).iterator(), emptyIterator()))));
        assertEquals(List.of(1), collect(concat(List.of(emptyIterator(), List.of(1).iterator()))));

        assertEquals(List.of(1, 2, 3), collect(concat(List.of(List.of(1).iterator(), List.of(2, 3).iterator()))));
    }

    @Test
    void testConcatList() {
        assertTrue(concat(new List[0]).isEmpty());
        assertTrue(concat((List<Object>[]) null).isEmpty());
        assertTrue(concat((List<Object>) List.of()).isEmpty());

        assertEquals(List.of(1), concat(List.of(1), List.of()));
        assertEquals(List.of(1), concat(List.of(), List.of(1)));

        assertEquals(List.of(1, 2), concat(List.of(1), List.of(2)));
        assertEquals(List.of(1, 2, 2), concat(List.of(1), List.of(2), List.of(2)));

        assertFalse(concat(new List[0]).contains(0));
        assertFalse(concat((List<Object>) List.of()).contains(0));
        assertFalse(concat(List.of(1)).contains(0));
        assertFalse(concat(List.of(1), List.of()).contains(0));
        assertFalse(concat(List.of(), List.of(1)).contains(0));
        assertFalse(concat(List.of(1), List.of(2, 3)).contains(0));

        assertTrue(concat(List.of(0)).contains(0));
        assertTrue(concat(List.of(), List.of(0)).contains(0));
        assertTrue(concat(List.of(0), List.of()).contains(0));

        assertEquals(0, concat(new List[0]).size());
        assertEquals(0, concat((List<Object>) List.of()).size());
        assertEquals(1, concat(List.of(1)).size());
        assertEquals(1, concat(List.of(), List.of(1)).size());
        assertEquals(1, concat(List.of(1), List.of()).size());
        assertEquals(2, concat(List.of(1), List.of(2)).size());
        assertEquals(3, concat(List.of(1), List.of(2, 3)).size());
        assertEquals(5, concat(List.of(1, 4, 5), List.of(2, 3)).size());

        List<Integer> integers = new ArrayList<>(List.of(1, 2, 3));

        List<Integer> union0 = concat(integers);

        integers.remove(1);

        assertEquals(2, union0.size());

        List<Integer> union1 = concat(List.of(0), List.of(1, 2), List.of(3, 4, 5));

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

    @Test
    void testIntersect() {
        assertThat(intersect(Set.of(1, 2), Set.of(2, 3)), is(Set.of(2)));
        assertThat(intersect(Set.of(1, 2), Set.of(3, 4)), is(Set.of()));
    }

    @Test
    void testLast() {
        assertNull(last(List.of()));

        assertEquals(1, last(List.of(1)));
        assertEquals(2, last(List.of(1, 2)));
    }

    @Test
    void testMapIterableWithPredicate() {
        assertFalse(mapIterable(null, null, null).iterator().hasNext());
        assertFalse(mapIterable(Collections.emptyList(), null, null).iterator().hasNext());

        assertEquals(List.of(1), collect(mapIterable(List.of(1), null, null)));
        assertEquals(List.of(1), collect(mapIterable(List.of(1), identity(), null)));
        assertEquals(List.of(1), collect(mapIterable(List.of(1), null, integer -> true)));
        assertEquals(List.of(1), collect(mapIterable(List.of(1), identity(), integer -> true)));
        assertEquals(List.of(), collect(mapIterable(List.of(1), null, integer -> false)));
        assertEquals(List.of(), collect(mapIterable(List.of(1), identity(), integer -> false)));

        assertEquals(List.of("1", "2", "3"), collect(mapIterable(List.of(1, 2, 3), String::valueOf, null)));
        assertEquals(List.of("3"), collect(mapIterable(List.of(1, 2, 3), String::valueOf, integer -> integer > 2)));

        Iterator<String> iterator1 = mapIterable(List.of(1, 2, 3, 4), String::valueOf, integer -> true).iterator();
        assertEquals("1", iterator1.next());
        assertEquals("2", iterator1.next());
        assertEquals("3", iterator1.next());
        assertEquals("4", iterator1.next());

        Iterator<String> iterator2 = mapIterable(List.of(1, 2, 3, 4), String::valueOf, null).iterator();
        assertEquals("1", iterator2.next());
        assertEquals("2", iterator2.next());
        assertEquals("3", iterator2.next());
        assertEquals("4", iterator2.next());

        Iterator<Integer> iterator3 = mapIterable(List.of(1, 2, 3, 4), identity(), integer -> integer < 3).iterator();
        assertEquals(1, iterator3.next());
        assertEquals(2, iterator3.next());

        Iterator<Integer> iterator4 = mapIterable(List.of(1, 2, 3, 4), identity(), integer -> false).iterator();
        assertFalse(iterator4.hasNext());

        assertDoesNotThrow(() -> mapIterable(Arrays.asList(new Integer[]{null}), null, null).iterator().next());
        assertDoesNotThrow(() -> mapIterable(Arrays.asList(new Integer[]{null}), null, integer -> true).iterator().next());
        assertDoesNotThrow(() -> mapIterable(Arrays.asList(null, 1), null, null).iterator().next());
        assertDoesNotThrow(() -> mapIterable(Arrays.asList(null, 1), null, integer -> true).iterator().next());

        assertThrows(
                NoSuchElementException.class,
                () -> mapIterable(Arrays.asList(new Integer[]{null}), null, integer -> false).iterator().next()
        );

        assertThrows(
                NoSuchElementException.class,
                () -> {
                    Iterator<Object> iterator = mapIterable(Arrays.asList(null, 1), null, Objects::nonNull).iterator();
                    iterator.next();
                    iterator.next();
                }
        );

        assertThrows(UnsupportedOperationException.class, () -> mapIterable(List.of(1), null, null).iterator().remove());
    }

    @Test
    void testViewList() {
        assertThat(view(List.of(), identity()), empty());
        assertThat(view(List.of(), Object::toString), empty());

        assertThat(view(List.of(1, 2, 3), identity()), equalTo(List.of(1, 2, 3)));
        assertThat(view(List.of(1, 2, 3), Integer::longValue), equalTo(List.of(1L, 2L, 3L)));

        List<Integer> list = new ArrayList<>(List.of(1));
        List<Integer> view = view(list, identity());

        assertThrows(UnsupportedOperationException.class, () -> view.add(0));
        assertThrows(UnsupportedOperationException.class, () -> view.set(0, 0));
        assertThrows(UnsupportedOperationException.class, () -> view.remove(0));

        list.add(2);
        assertThat(view, equalTo(List.of(1, 2)));
    }
}
