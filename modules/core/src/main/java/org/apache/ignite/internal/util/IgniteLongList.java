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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.lang.IgniteStringBuilder;
import org.jetbrains.annotations.Nullable;

/**
 * Minimal list API to work with primitive longs. This list exists to avoid boxing/unboxing when using standard list from Java.
 */
// TODO: https://issues.apache.org/jira/browse/IGNITE-16453
public class IgniteLongList implements Externalizable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Empty array. */
    public static final long[] EMPTY_ARRAY = new long[0];

    /** Internal array. */
    private long[] arr;

    /** Size of the list. */
    private int size;

    /**
     * Default constructor.
     */
    public IgniteLongList() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param size Initial size of the list.
     */
    public IgniteLongList(int size) {
        this(new long[size], 0);
    }

    /**
     * Constructor.
     *
     * <p>NOTE: Without copying the array.
     *
     * @param arr Initial array of the list.
     */
    public IgniteLongList(long[] arr) {
        this(arr, arr.length);
    }

    /**
     * Returns new list from values.
     *
     * <p>NOTE: Without copying the array.
     *
     * @param vals Values.
     */
    public static IgniteLongList asList(@Nullable long... vals) {
        if (ArrayUtils.nullOrEmpty(vals)) {
            return new IgniteLongList();
        }

        return new IgniteLongList(vals);
    }

    /**
     * Constructor.
     *
     * <p>NOTE: Without copying the array.
     *
     * @param arr Initial array of the list.
     * @param size Initial size of the list.
     */
    private IgniteLongList(long[] arr, int size) {
        this.arr = arr;
        this.size = size;
    }

    /**
     * Returns copy of this list.
     */
    public IgniteLongList copy() {
        if (size == 0) {
            return new IgniteLongList();
        }

        return new IgniteLongList(Arrays.copyOf(arr, size));
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof IgniteLongList)) {
            return false;
        }

        IgniteLongList that = (IgniteLongList) o;

        if (size != that.size) {
            return false;
        }

        if (size == 0 || arr == that.arr) {
            return true;
        }

        for (int i = 0; i < size; i++) {
            if (arr[i] != that.arr[i]) {
                return false;
            }
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        int res = 1;

        for (int i = 0; i < size; i++) {
            long element = arr[i];
            int elementHash = (int) (element ^ (element >>> 32));
            res = 31 * res + elementHash;
        }

        return res;
    }

    /**
     * Appends all the elements in the specified collection to the end of this list.
     *
     * @param l Another list.
     */
    public void addAll(IgniteLongList l) {
        if (l.isEmpty()) {
            return;
        }

        if (arr == null) {
            arr = new long[4];
        }

        int len = arr.length;

        while (len < size + l.size()) {
            len <<= 1;
        }

        arr = Arrays.copyOf(arr, len);

        System.arraycopy(l.arr, 0, arr, size, l.size());

        size += l.size();
    }

    /**
     * Moves elements from {@code from} to {@code to}.
     *
     * @param to To list.
     * @param from From list.
     * @return To list (passed in or created).
     */
    public static IgniteLongList addAll(@Nullable IgniteLongList to, IgniteLongList from) {
        if (to == null) {
            IgniteLongList res = new IgniteLongList(from.size());

            res.addAll(from);

            return res;
        } else {
            to.addAll(from);

            return to;
        }
    }

    /**
     * Add element to this array.
     *
     * @param x Value.
     */
    public void add(long x) {
        if (arr == null) {
            arr = new long[4];
        } else if (arr.length == size) {
            arr = Arrays.copyOf(arr, arr.length << 1);
        }

        arr[size++] = x;
    }

    /**
     * Clears the list.
     */
    public void clear() {
        size = 0;
    }

    /**
     * Gets the last element.
     *
     * @return The last element.
     */
    public long last() {
        return arr[size - 1];
    }

    /**
     * Removes and returns the last element of the list. Complementary method to {@link #add(long)} for stack like usage.
     *
     * @return Removed element.
     * @throws NoSuchElementException If the list is empty.
     */
    public long remove() throws NoSuchElementException {
        if (size == 0) {
            throw new NoSuchElementException();
        }

        return arr[--size];
    }

    /**
     * Returns (possibly reordered) copy of this list, excluding all elements of given list.
     *
     * @param l List of elements to remove.
     * @return New list without all elements from {@code l}.
     */
    public IgniteLongList copyWithout(IgniteLongList l) {
        assert l != null;

        if (size == 0) {
            return new IgniteLongList();
        }

        if (l.size == 0) {
            return new IgniteLongList(Arrays.copyOf(arr, size));
        }

        long[] newArr = Arrays.copyOf(arr, size);
        int newIdx = size;

        for (int i = 0; i < l.size(); i++) {
            long rmVal = l.get(i);

            for (int j = 0; j < newIdx; j++) {
                if (newArr[j] == rmVal) {

                    while (newIdx > 0 && newArr[newIdx - 1] == rmVal) {
                        newIdx--;
                    }

                    if (newIdx > 0) {
                        newArr[j] = newArr[newIdx - 1];
                        newIdx--;
                    }
                }
            }
        }

        return new IgniteLongList(newArr, newIdx);
    }

    /**
     * Returns the element at the specified position in this list.
     *
     * @param i Index.
     */
    public long get(int i) {
        assert i < size;

        return arr[i];
    }

    /**
     * Returns the size of the list.
     */
    public int size() {
        return size;
    }

    /**
     * Returns {@code true} if this list has no elements.
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Returns {@code true} if found an element in the list.
     *
     * @param l Element to find.
     */
    public boolean contains(long l) {
        for (int i = 0; i < size; i++) {
            if (arr[i] == l) {
                return true;
            }
        }

        return false;
    }

    /**
     * Returns {@code true} if found all elements in the list.
     *
     * @param l List to check.
     */
    public boolean containsAll(IgniteLongList l) {
        for (int i = 0; i < l.size(); i++) {
            if (!contains(l.get(i))) {
                return false;
            }
        }

        return true;
    }

    /**
     * Returns {@code true} if there are no duplicates.
     */
    public boolean distinct() {
        for (int i = 0; i < size; i++) {
            for (int j = i + 1; j < size; j++) {
                if (arr[i] == arr[j]) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Trims this list to a new size.
     *
     * @param size New size.
     * @param last If {@code true} the last elements will be removed, otherwise the first.
     */
    public void truncate(int size, boolean last) {
        assert size >= 0 && size <= this.size;

        if (size == this.size) {
            return;
        }

        if (!last && this.size != 0 && size != 0) {
            System.arraycopy(arr, this.size - size, arr, 0, size);
        }

        this.size = size;
    }

    /**
     * Removes element by given index.
     *
     * @param i Index.
     * @return Removed value.
     */
    public long removeIndex(int i) {
        assert i < size : i;

        long res = arr[i];

        if (i == size - 1) { // Last element.
            size = i;
        } else {
            System.arraycopy(arr, i + 1, arr, i, size - i - 1);
            size--;
        }

        return res;
    }

    /**
     * Removes value from this list.
     *
     * @param startIdx Index to begin search with.
     * @param val Value.
     * @return Index of removed value if the value was found and removed or {@code -1} otherwise.
     */
    public int removeValue(int startIdx, long val) {
        assert startIdx >= 0;

        for (int i = startIdx; i < size; i++) {
            if (arr[i] == val) {
                removeIndex(i);

                return i;
            }
        }

        return -1;
    }

    /**
     * Removes value from this list.
     *
     * @param startIdx Index to begin search with.
     * @param oldVal Old value.
     * @param newVal New value.
     * @return Index of replaced value if the value was found and replaced or {@code -1} otherwise.
     */
    public int replaceValue(int startIdx, long oldVal, long newVal) {
        for (int i = startIdx; i < size; i++) {
            if (arr[i] == oldVal) {
                arr[i] = newVal;

                return i;
            }
        }

        return -1;
    }

    /**
     * Returns array copy.
     */
    public long[] array() {
        if (arr == null) {
            return EMPTY_ARRAY;
        }

        long[] res = new long[size];

        System.arraycopy(arr, 0, res, 0, size);

        return res;
    }

    /** {@inheritDoc} */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(size);

        for (int i = 0; i < size; i++) {
            out.writeLong(arr[i]);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        size = in.readInt();

        arr = new long[size];

        for (int i = 0; i < size; i++) {
            arr[i] = in.readLong();
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        IgniteStringBuilder b = new IgniteStringBuilder("[");

        for (int i = 0; i < size; i++) {
            if (i != 0) {
                b.app(',');
            }

            b.app(arr[i]);
        }

        b.app(']');

        return S.toString(IgniteLongList.class, this, "arr", b);
    }

    /**
     * Reads a list from {@link DataInput}.
     *
     * @param in Input to read list from.
     * @return Grid long list.
     * @throws IOException If failed.
     */
    @Nullable
    public static IgniteLongList readFrom(DataInput in) throws IOException {
        int idx = in.readInt();

        if (idx == -1) {
            return null;
        }

        long[] arr = new long[idx];

        for (int i = 0; i < idx; i++) {
            arr[i] = in.readLong();
        }

        return new IgniteLongList(arr);
    }

    /**
     * Writes a list to {@link DataOutput}.
     *
     * @param out Output to write to.
     * @param list List.
     * @throws IOException If failed.
     */
    public static void writeTo(DataOutput out, @Nullable IgniteLongList list) throws IOException {
        out.writeInt(list != null ? list.size : -1);

        if (list != null) {
            for (int i = 0; i < list.size; i++) {
                out.writeLong(list.arr[i]);
            }
        }
    }

    /**
     * Sorts this list. Use {@code copy().sort()} if you need a defensive copy.
     *
     * @return {@code this} For chaining.
     */
    public IgniteLongList sort() {
        if (size > 1) {
            Arrays.sort(arr, 0, size);
        }

        return this;
    }

    /**
     * Removes given number of elements from the end. If the given number of elements is higher than list size, then list will be cleared.
     *
     * @param cnt Count to pop from the end.
     */
    public void pop(int cnt) {
        assert cnt >= 0 : cnt;

        if (size < cnt) {
            size = 0;
        } else {
            size -= cnt;
        }
    }
}
