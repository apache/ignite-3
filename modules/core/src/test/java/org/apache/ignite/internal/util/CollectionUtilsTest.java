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
        assertTrue(union().isEmpty());
        assertTrue(union((Collection<Object>[]) null).isEmpty());
        assertTrue(union(List.of()).isEmpty());

        assertEquals(List.of(1), collect(union(List.of(1), List.of())));
        assertEquals(List.of(1), collect(union(List.of(), List.of(1))));

        assertEquals(List.of(1, 2), collect(union(List.of(1), List.of(2))));
        assertEquals(List.of(1, 2, 2), collect(union(List.of(1), List.of(2), List.of(2))));

        assertFalse(union().contains(0));
        assertFalse(union(List.of()).contains(0));
        assertFalse(union(List.of(1)).contains(0));
        assertFalse(union(List.of(1), List.of()).contains(0));
        assertFalse(union(List.of(), List.of(1)).contains(0));
        assertFalse(union(List.of(1), List.of(2, 3)).contains(0));

        assertTrue(union(List.of(0)).contains(0));
        assertTrue(union(List.of(), List.of(0)).contains(0));
        assertTrue(union(List.of(0), List.of()).contains(0));

        assertEquals(0, union().size());
        assertEquals(0, union(List.of()).size());
        assertEquals(1, union(List.of(1)).size());
        assertEquals(1, union(List.of(), List.of(1)).size());
        assertEquals(1, union(List.of(1), List.of()).size());
        assertEquals(2, union(List.of(1), List.of(2)).size());
        assertEquals(3, union(List.of(1), List.of(2, 3)).size());
        assertEquals(5, union(List.of(1, 4, 5), List.of(2, 3)).size());

        List<Integer> integers = new ArrayList<>(List.of(1, 2, 3));

        Collection<Integer> union = union(integers);

        integers.remove(0);

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
