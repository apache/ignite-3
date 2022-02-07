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

import static org.apache.ignite.internal.util.IgniteLongList.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Class to test the {@link IgniteLongList}.
 */
public class IgniteLongListSelfTest {
    /** Size of the header when list is written with message writer. */
    private static final int HEADER_SIZE = 2;

    @SuppressWarnings("ZeroLengthArrayAllocation")
    @Test
    public void testCopyWithout() {
        assertCopy(
                new IgniteLongList(new long[]{}),
                new IgniteLongList(new long[]{})
        );

        assertCopy(
                new IgniteLongList(new long[]{}),
                new IgniteLongList(new long[]{1})
        );

        assertCopy(
                new IgniteLongList(new long[]{1}),
                new IgniteLongList(new long[]{}));

        assertCopy(
                new IgniteLongList(new long[]{1, 2, 3}),
                new IgniteLongList(new long[]{4, 5, 6})
        );

        assertCopy(
                new IgniteLongList(new long[]{1, 2, 3}),
                new IgniteLongList(new long[]{1, 2, 3})
        );

        assertCopy(
                new IgniteLongList(new long[]{1, 2, 3, 4, 5, 1}),
                new IgniteLongList(new long[]{1, 1})
        );

        assertCopy(
                new IgniteLongList(new long[]{1, 1, 1, 2, 3, 4, 5, 1, 1, 1}),
                new IgniteLongList(new long[]{1, 1})
        );

        assertCopy(
                new IgniteLongList(new long[]{1, 2, 3}),
                new IgniteLongList(new long[]{1, 1, 2, 2, 3, 3})
        );
    }

    @Test
    public void testTruncate() {
        IgniteLongList list = asList(1, 2, 3, 4, 5, 6, 7, 8);

        list.truncate(4, true);

        assertEquals(asList(1, 2, 3, 4), list);

        list.truncate(2, false);

        assertEquals(asList(3, 4), list);

        list = new IgniteLongList();

        list.truncate(0, false);
        list.truncate(0, true);

        assertEquals(new IgniteLongList(), list);
    }

    @Test
    public void testRemove() {
        IgniteLongList list = asList(1, 2, 3, 4, 5, 6);

        assertEquals(2, list.removeValue(0, 3));
        assertEquals(asList(1, 2, 4, 5, 6), list);

        assertEquals(-1, list.removeValue(1, 1));
        assertEquals(-1, list.removeValue(0, 3));

        assertEquals(4, list.removeValue(0, 6));
        assertEquals(asList(1, 2, 4, 5), list);

        assertEquals(2, list.removeIndex(1));
        assertEquals(asList(1, 4, 5), list);

        assertEquals(1, list.removeIndex(0));
        assertEquals(asList(4, 5), list);
    }

    @Test
    public void testSort() {
        assertEquals(new IgniteLongList(), new IgniteLongList().sort());
        assertEquals(asList(1), asList(1).sort());
        assertEquals(asList(1, 2), asList(2, 1).sort());
        assertEquals(asList(1, 2, 3), asList(2, 1, 3).sort());

        IgniteLongList list = new IgniteLongList();

        list.add(4);
        list.add(3);
        list.add(5);
        list.add(1);

        assertEquals(asList(1, 3, 4, 5), list.sort());

        list.add(0);

        assertEquals(asList(1, 3, 4, 5, 0), list);
        assertEquals(asList(0, 1, 3, 4, 5), list.sort());
    }

    @Test
    public void testArray() {
        IgniteLongList list = new IgniteLongList();

        long[] array = list.array();

        assertNotNull(array);

        assertEquals(0, array.length);

        list.add(1L);

        array = list.array();

        assertNotNull(array);

        assertEquals(1, array.length);

        assertEquals(1L, array[0]);
    }

    private void assertCopy(IgniteLongList lst, IgniteLongList rmv) {
        IgniteLongList res = lst.copyWithout(rmv);

        for (int i = 0; i < lst.size(); i++) {
            long v = lst.get(i);

            if (rmv.contains(v)) {
                assertFalse(res.contains(v));
            } else {
                assertTrue(res.contains(v));
            }
        }
    }
}
