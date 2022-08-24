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

import static org.apache.ignite.internal.storage.rocksdb.index.CursorUtils.concat;
import static org.apache.ignite.internal.storage.rocksdb.index.CursorUtils.dropWhile;
import static org.apache.ignite.internal.storage.rocksdb.index.CursorUtils.map;
import static org.apache.ignite.internal.storage.rocksdb.index.CursorUtils.takeWhile;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import org.apache.ignite.internal.util.Cursor;
import org.junit.jupiter.api.Test;

/**
 * Class for testing {@link CursorUtils}.
 */
public class CursorUtilsTest {
    @Test
    public void testDropWhile() {
        Cursor<Integer> actual = dropWhile(cursor(1, 2, 5, 14, 20, 1), i -> i < 10);

        assertThat(actual, contains(14, 20, 1));

        assertThat(dropWhile(cursor(), i -> i.hashCode() < 10), is(emptyIterable()));
    }

    @Test
    public void testTakeWhile() {
        Cursor<Integer> actual = takeWhile(cursor(1, 2, 5, 14, 20, 1), i -> i < 10);

        assertThat(actual, contains(1, 2, 5));

        assertThat(takeWhile(cursor(), i -> i.hashCode() < 10), is(emptyIterable()));
    }

    @Test
    public void testMap() {
        Cursor<String> actual = map(cursor(1, 2, 5, 14, 20), i -> "foo" + i);

        assertThat(actual, contains("foo1", "foo2", "foo5", "foo14", "foo20"));

        assertThat(map(cursor(), Object::toString), is(emptyIterable()));
    }

    @Test
    public void testConcat() {
        Cursor<Integer> actual = concat(cursor(1, 2, 5), cursor(5, 2, 1));

        assertThat(actual, contains(1, 2, 5, 5, 2, 1));

        assertThat(concat(cursor(), cursor()), is(emptyIterable()));
    }

    @Test
    public void testCombination() {
        Cursor<Integer> dropWhile = dropWhile(cursor(1, 5, 8, 10, 42), i -> i <= 8);

        Cursor<Integer> takeWhile = takeWhile(dropWhile, i -> i >= 10);

        Cursor<Integer> concat = concat(takeWhile, cursor(1, 2, 3));

        Cursor<String> map = map(concat, String::valueOf);

        assertThat(map, contains("10", "42", "1", "2", "3"));
    }

    @SafeVarargs
    private static <T> Cursor<T> cursor(T... elements) {
        return Cursor.fromIterator(Arrays.asList(elements).iterator());
    }
}
