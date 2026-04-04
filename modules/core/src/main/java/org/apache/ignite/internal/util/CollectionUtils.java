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
import static java.util.Collections.nCopies;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import java.util.AbstractCollection;
import java.util.AbstractList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class provides various method to work with collections.
 */
public final class CollectionUtils {
    /** Special object for determining that there is no next element. */
    private static final Object NO_NEXT_ELEMENT = new Object();

    /** Stub. */
    private CollectionUtils() {
        // No op.
    }

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
     * Tests if the given iterator is either {@code null} or empty.
     *
     * @param iter Iterator.
     * @return Whether or not the given iterator is {@code null} or empty.
     */
    public static boolean nullOrEmpty(@Nullable Iterator<?> iter) {
        return iter == null || !iter.hasNext();
    }

    /**
     * Count values in iterator.
     *
     * @param iter The iterator.
     *
     * @return The count.
     */
    public static int count(@Nullable Iterator<?> iter) {
        if (iter == null) {
            return 0;
        }

        int cnt = 0;

        while (iter.hasNext()) {
            iter.next();
            cnt++;
        }

        return cnt;
    }

    /**
     * Gets first element from given list or returns {@code null} if list is empty.
     *
     * @param list List to retrieve the first element.
     * @param <T> Type of the elements of the list.
     * @return The first element of the given list or {@code null} in case the list is empty.
     */
    public static <T> @Nullable T first(List<? extends T> list) {
        if (nullOrEmpty(list)) {
            return null;
        }

        return list.get(0);
    }

    /**
     * Gets first element from given collection or returns {@code null} if collection is empty.
     *
     * @param col Collection to retrieve the first element.
     * @param <T> Type of the elements of the collection.
     * @return The first element of the given collection or {@code null} in case the collection is empty.
     */
    public static <T> @Nullable T first(Collection<? extends T> col) {
        if (nullOrEmpty(col)) {
            return null;
        }

        return col.iterator().next();
    }

    /**
     * Returns first element from the given iterable or returns {@code null} if the list is empty.
     *
     * @param iterable Iterable to retrieve the first element.
     * @param <T> Type of the elements of the list.
     * @return The first element of the given iterable or {@code null} in case the iterable is null or empty.
     */
    public static <T> @Nullable T first(Iterable<? extends T> iterable) {
        if (iterable == null) {
            return null;
        }

        Iterator<? extends T> it = iterable.iterator();

        if (!it.hasNext()) {
            return null;
        }

        return it.next();
    }

    /**
     * Logical union operation on two probably {@code null} or empty sets.
     *
     * @param firstSet First operand.
     * @param secondSet Second operand.
     * @return Result of the union on two sets that equals to all unique elements from the first and the second set or empty set if both
     *      given sets are empty.
     */
    public static <T> Set<T> union(@Nullable Set<T> firstSet, @Nullable Set<T> secondSet) {
        boolean isFirstSetEmptyOrNull = nullOrEmpty(firstSet);
        boolean isSecondSetEmptyOrNull = nullOrEmpty(secondSet);

        if (isFirstSetEmptyOrNull && isSecondSetEmptyOrNull) {
            return Set.of();
        } else if (isFirstSetEmptyOrNull) {
            return unmodifiableSet(secondSet);
        } else if (isSecondSetEmptyOrNull) {
            return unmodifiableSet(firstSet);
        } else {
            var union = new HashSet<>(firstSet);

            union.addAll(secondSet);

            return unmodifiableSet(union);
        }
    }

    /**
     * Concatenates collections.
     *
     * @param collections Collections.
     * @param <T> Type of the elements of lists.
     * @return Immutable collection concatenation.
     */
    @SafeVarargs
    public static <T> Collection<T> concat(Collection<T>... collections) {
        if (collections == null || collections.length == 0) {
            return List.of();
        }

        return new AbstractCollection<>() {
            /** {@inheritDoc} */
            @Override
            public Iterator<T> iterator() {
                return concat((Iterable<T>[]) collections).iterator();
            }

            /** {@inheritDoc} */
            @Override
            public int size() {
                int size = 0;

                for (int i = 0; i < collections.length; i++) {
                    size += collections[i].size();
                }

                return size;
            }

            /** {@inheritDoc} */
            @Override
            public boolean contains(Object o) {
                for (int i = 0; i < collections.length; i++) {
                    if (collections[i].contains(o)) {
                        return true;
                    }
                }

                return false;
            }
        };
    }

    /**
     * Create a lazy concatenation of iterables.
     *
     * <p>NOTE: {@link Iterator#remove} - not supported.
     *
     * @param iterables Iterables.
     * @param <T> Type of the elements.
     * @return Concatenation of iterables.
     */
    @SafeVarargs
    public static <T> Iterable<T> concat(@Nullable Iterable<? extends T>... iterables) {
        if (iterables == null || iterables.length == 0) {
            return Collections::emptyIterator;
        }

        return () -> new Iterator<>() {
            /** Current index at {@code iterables}. */
            int idx = 0;

            /** Current iterator. */
            Iterator<? extends T> curr = emptyIterator();

            /** {@inheritDoc} */
            @Override
            public boolean hasNext() {
                while (!curr.hasNext() && idx < iterables.length) {
                    curr = iterables[idx++].iterator();
                }

                return curr.hasNext();
            }

            /** {@inheritDoc} */
            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                } else {
                    return curr.next();
                }
            }
        };
    }

    /**
     * Create a lazy concatenation of iterators.
     *
     * <p>NOTE: {@link Iterator#remove} - not supported.
     *
     * @param iterators Iterators.
     * @param <T> Type of the elements.
     * @return Concatenation of iterators.
     */
    @SafeVarargs
    public static <T> Iterator<T> concat(@Nullable Iterator<? extends T>... iterators) {
        if (iterators == null || iterators.length == 0) {
            return emptyIterator();
        }

        return new Iterator<>() {
            /** Current index at {@code iterators}. */
            int idx = 0;

            /** Current iterator. */
            Iterator<? extends T> curr = emptyIterator();

            /** {@inheritDoc} */
            @Override
            public boolean hasNext() {
                while (!curr.hasNext() && idx < iterators.length) {
                    curr = iterators[idx++];
                }

                return curr.hasNext();
            }

            /** {@inheritDoc} */
            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                } else {
                    return curr.next();
                }
            }
        };
    }

    /**
     * Create a lazy concatenation of iterators.
     *
     * <p>NOTE: {@link Iterator#remove} - not supported.
     *
     * @param iterators Iterators.
     * @param <T> Type of the elements.
     * @return Concatenation of iterators.
     */
    public static <T> Iterator<T> concat(@Nullable Iterable<Iterator<? extends T>> iterators) {
        if (iterators == null) {
            return emptyIterator();
        }

        return concat(iterators.iterator());
    }

    /**
     * Create a lazy concatenation of iterators.
     *
     * <p>NOTE: {@link Iterator#remove} - not supported.
     *
     * @param iterators Iterators.
     * @param <T> Type of the elements.
     * @return Concatenation of iterators.
     */
    public static <T> Iterator<T> concat(@Nullable Iterator<Iterator<? extends T>> iterators) {
        if (iterators == null || !iterators.hasNext()) {
            return emptyIterator();
        }

        return new Iterator<>() {

            /** Current iterator. */
            Iterator<? extends T> curr = emptyIterator();

            /** {@inheritDoc} */
            @Override
            public boolean hasNext() {
                while (!curr.hasNext() && iterators.hasNext()) {
                    curr = iterators.next();
                }

                return curr.hasNext();
            }

            /** {@inheritDoc} */
            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                } else {
                    return curr.next();
                }
            }
        };
    }

    /**
     * Concatenates lists.
     *
     * @param lists Lists.
     * @param <T> Type of the elements of lists.
     * @return Immutable list concatenation.
     */
    @SafeVarargs
    public static <T> List<T> concat(List<T>... lists) {
        if (lists == null || lists.length == 0) {
            return List.of();
        }

        return new AbstractList<>() {
            /** {@inheritDoc} */
            @Override
            public T get(int index) {
                for (List<T> list : lists) {
                    if (index >= list.size()) {
                        index -= list.size();
                    } else {
                        return list.get(index);
                    }
                }

                throw new IndexOutOfBoundsException(index);
            }

            /** {@inheritDoc} */
            @Override
            public Iterator<T> iterator() {
                return concat((Iterable<T>[]) lists).iterator();
            }

            /** {@inheritDoc} */
            @Override
            public int size() {
                int size = 0;

                for (List<T> list : lists) {
                    size += list.size();
                }

                return size;
            }

            /** {@inheritDoc} */
            @Override
            public boolean contains(Object o) {
                for (List<T> list : lists) {
                    if (list.contains(o)) {
                        return true;
                    }
                }

                return false;
            }
        };
    }

    /**
     * Difference of two sets.
     *
     * @param a First set.
     * @param b Second set.
     * @param <T> Type of the elements.
     * @return Immutable set of elements of the first without the second.
     */
    public static <T> Set<T> difference(@Nullable Set<T> a, @Nullable Set<T> b) {
        if (nullOrEmpty(a)) {
            return Set.of();
        } else if (nullOrEmpty(b)) {
            return unmodifiableSet(a);
        }

        // Lazy initialization.
        Set<T> res = null;

        for (T t : a) {
            if (!b.contains(t)) {
                if (res == null) {
                    res = new HashSet<>();
                }

                res.add(t);
            }
        }

        return res == null ? Set.of() : unmodifiableSet(res);
    }

    /**
     * Get unmodifiable copy set of specified collection.
     *
     * @param coll Collection of integer to copy.
     * @return Unmodifiable copy set.
     */
    public static IntSet setOf(Collection<Integer> coll) {
        return IntSets.unmodifiable(new IntOpenHashSet(coll));
    }

    /**
     * Returns an intersection of two sets.
     *
     * @param op1 First operand.
     * @param op2 Second operand.
     * @return Result of the intersection.
     */
    public static <T> Set<T> intersect(Set<T> op1, Set<T> op2) {
        return op1.stream().filter(op2::contains).collect(toSet());
    }

    /**
     * Gets last element from given list or returns {@code null} if list is empty.
     *
     * @param list List to retrieve the last element.
     * @param <T> Type of the elements of the list.
     */
    public static <T> @Nullable T last(List<? extends T> list) {
        if (list.isEmpty()) {
            return null;
        }

        return list.get(list.size() - 1);
    }

    /**
     * Returns a {@code Collector} that accumulates elements into a {@link Int2ObjectOpenHashMap}.
     *
     * @param keyMapper Key mapper.
     * @param valueMapper Value mapper.
     * @param <T> the type of the input elements.
     * @param <V> the output type of the value mapping function.
     * @return Map collector.
     */
    public static <T, V> Collector<T, ?, Int2ObjectMap<V>> toIntMapCollector(
            Function<T, Integer> keyMapper, Function<T, V> valueMapper) {
        return Collectors.toMap(
                keyMapper,
                valueMapper,
                (oldVal, newVal) -> newVal,
                Int2ObjectOpenHashMap::new
        );
    }

    /** Returns immutable copy of the given list or {@code null} if the list is null. */
    @Contract("!null -> !null")
    public static <T> @Nullable List<T> copyOrNull(@Nullable List<T> list) {
        if (list == null) {
            return null;
        }

        return List.copyOf(list);
    }

    /** Returns immutable copy of the given set or {@code null} if the set is null. */
    @Contract("!null -> !null")
    public static <T> @Nullable Set<T> copyOrNull(@Nullable Set<T> set) {
        if (set == null) {
            return null;
        }

        return Set.copyOf(set);
    }

    /**
     * Maps iterable via provided mapper function.
     *
     * @param iterable Basic iterable.
     * @param mapper Conversion function.
     * @param predicate Predicate to apply to each element of basic iterable.
     * @param <T1> Base type of the iterable.
     * @param <T2> Type for view.
     * @return Mapped iterable.
     */
    public static <T1, T2> Iterable<T2> mapIterable(
            @Nullable Iterable<? extends T1> iterable,
            @Nullable Function<? super T1, ? extends T2> mapper,
            @Nullable Predicate<? super T1> predicate
    ) {
        if (iterable == null) {
            return Collections.emptyList();
        }

        if (mapper == null && predicate == null) {
            return (Iterable<T2>) iterable;
        }

        return new Iterable<>() {
            @Override
            public Iterator<T2> iterator() {
                Iterator<? extends T1> innerIterator = iterable.iterator();
                return new Iterator<>() {
                    @Nullable
                    T1 current = advance();

                    /** {@inheritDoc} */
                    @Override
                    public boolean hasNext() {
                        return current != NO_NEXT_ELEMENT;
                    }

                    /** {@inheritDoc} */
                    @Override
                    public T2 next() {
                        T1 current = this.current;

                        if (current == NO_NEXT_ELEMENT) {
                            throw new NoSuchElementException();
                        }

                        this.current = advance();

                        return mapper == null ? (T2) current : mapper.apply(current);
                    }

                    private @Nullable T1 advance() {
                        while (innerIterator.hasNext()) {
                            T1 next = innerIterator.next();

                            if (predicate == null || predicate.test(next)) {
                                return next;
                            }
                        }

                        return (T1) NO_NEXT_ELEMENT;
                    }
                };
            }
        };
    }

    /**
     * Creates a view of the original list without copying it with another type from the map function.
     *
     * @param list Original list.
     * @param mapper Map function.
     * @param <T> Type of the input elements.
     * @param <R> Output type of the value mapping function.
     */
    public static <T, R> List<R> view(List<T> list, Function<? super T, ? extends R> mapper) {
        if (list.isEmpty()) {
            return List.of();
        }

        return new AbstractList<>() {
            @Override
            public R get(int index) {
                return mapper.apply(list.get(index));
            }

            @Override
            public int size() {
                return list.size();
            }
        };
    }

    /**
     * Sets list element at the specified index. Expands a list if needed.
     *
     * @param list List to update.
     * @param i Target index.
     * @param element Element to put.
     * @param <T> Type of the list elements.
     */
    public static <T> void setListAtIndex(List<T> list, int i, T element) {
        if (list.size() < i) {
            list.addAll(nCopies(i - list.size(), null));
        }

        if (list.size() < i + 1) {
            list.add(element);
        } else {
            T prev = list.set(i, element);

            assert prev == null : String.format("Found previous value %s at index %d", prev, i);
        }
    }
}
