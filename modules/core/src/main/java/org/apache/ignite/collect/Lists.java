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

package org.apache.ignite.collect;

import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.RandomAccess;

import org.apache.ignite.lang.Preconditions;
import org.jetbrains.annotations.Nullable;

/**
 * Static utility methods pertaining to {@link List} instances.
 */
public final class Lists {
    private Lists() {
    }

    /**
     * Returns a reversed view of the specified list.
     */
    public static <T> List<T> reverse(List<T> list) {
        if (list instanceof RandomAccess)
            return new RandomAccessReverseList<>(list);
        else
            return new ReverseList<>(list);
    }

    private static class ReverseList<T> extends AbstractList<T> {
        private final List<T> forwardList;

        ReverseList(List<T> forwardList) {
            this.forwardList = Objects.requireNonNull(forwardList);
        }

        List<T> getForwardList() {
            return forwardList;
        }

        private int reverseIndex(int index) {
            int size = size();
            Preconditions.checkElementIndex(index, size);
            return (size - 1) - index;
        }

        private int reversePosition(int index) {
            int size = size();
            Preconditions.checkPositionIndex(index, size);
            return size - index;
        }

        @Override public void add(int index, @Nullable T element) {
            forwardList.add(reversePosition(index), element);
        }

        @Override public void clear() {
            forwardList.clear();
        }

        @Override public T remove(int index) {
            return forwardList.remove(reverseIndex(index));
        }

        @Override protected void removeRange(int fromIndex, int toIndex) {
            subList(fromIndex, toIndex).clear();
        }

        @Override public T set(int index, @Nullable T element) {
            return forwardList.set(reverseIndex(index), element);
        }

        @Override public T get(int index) {
            return forwardList.get(reverseIndex(index));
        }

        @Override public int size() {
            return forwardList.size();
        }

        @Override public List<T> subList(int fromIndex, int toIndex) {
            Preconditions.checkPositionIndexes(fromIndex, toIndex, size());
            return reverse(forwardList.subList(reversePosition(toIndex), reversePosition(fromIndex)));
        }

        @Override public Iterator<T> iterator() {
            return listIterator();
        }

        @Override public ListIterator<T> listIterator(int index) {
            int start = reversePosition(index);
            final ListIterator<T> forwardIterator = forwardList.listIterator(start);
            return new ListIterator<T>() {

                boolean canRemoveOrSet;

                @Override public void add(T e) {
                    forwardIterator.add(e);
                    forwardIterator.previous();
                    canRemoveOrSet = false;
                }

                @Override public boolean hasNext() {
                    return forwardIterator.hasPrevious();
                }

                @Override public boolean hasPrevious() {
                    return forwardIterator.hasNext();
                }

                @Override public T next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    canRemoveOrSet = true;
                    return forwardIterator.previous();
                }

                @Override public int nextIndex() {
                    return reversePosition(forwardIterator.nextIndex());
                }

                @Override public T previous() {
                    if (!hasPrevious()) {
                        throw new NoSuchElementException();
                    }
                    canRemoveOrSet = true;
                    return forwardIterator.next();
                }

                @Override public int previousIndex() {
                    return nextIndex() - 1;
                }

                @Override public void remove() {
                    Preconditions.checkRemove(canRemoveOrSet);
                    forwardIterator.remove();
                    canRemoveOrSet = false;
                }

                @Override public void set(T e) {
                    Preconditions.checkState(canRemoveOrSet);
                    forwardIterator.set(e);
                }
            };
        }
    }

    private static class RandomAccessReverseList<T> extends ReverseList<T> implements RandomAccess {
        RandomAccessReverseList(List<T> forwardList) {
            super(forwardList);
        }
    }
}
