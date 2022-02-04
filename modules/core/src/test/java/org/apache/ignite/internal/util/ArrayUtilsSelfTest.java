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

import static org.apache.ignite.internal.util.ArrayUtils.remove;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.junit.jupiter.api.Test;

/**
 * Class to test the {@link ArrayUtils}.
 */
public class ArrayUtilsSelfTest {
    @Test
    public void testRemoveLong() {
        long[] arr = {0, 1, 2, 3, 4, 5, 6};

        assertArrayEquals(new long[]{1, 2, 3, 4, 5, 6}, remove(arr, 0));
        assertArrayEquals(new long[]{0, 2, 3, 4, 5, 6}, remove(arr, 1));
        assertArrayEquals(new long[]{0, 1, 2, 3, 5, 6}, remove(arr, 4));
        assertArrayEquals(new long[]{0, 1, 2, 3, 4, 5}, remove(arr, 6));
        assertArrayEquals(new long[0], remove(new long[]{1}, 0));
    }

    @Test
    public void testRemove() {
        Integer[] arr = {0, 1, 2, 3, 4, 5, 6};

        assertArrayEquals(new Integer[]{1, 2, 3, 4, 5, 6}, remove(arr, 0));
        assertArrayEquals(new Integer[]{0, 2, 3, 4, 5, 6}, remove(arr, 1));
        assertArrayEquals(new Integer[]{0, 1, 2, 3, 5, 6}, remove(arr, 4));
        assertArrayEquals(new Integer[]{0, 1, 2, 3, 4, 5}, remove(arr, 6));
        assertArrayEquals(new Integer[0], remove(new Integer[]{1}, 0));
    }
}
