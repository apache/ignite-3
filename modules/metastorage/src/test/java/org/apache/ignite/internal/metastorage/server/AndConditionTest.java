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

package org.apache.ignite.internal.metastorage.server;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.ArrayUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for 'and' compound condition.
 *
 * @see AndCondition
 */
public class AndConditionTest extends BaseIgniteAbstractTest {
    private Condition cond1;
    private Condition cond2;
    private Condition cond3;
    private Condition cond4;

    private final Entry[] entries = {
            new EntryImpl(new byte[]{1}, new byte[]{10}, 1, 1),
            new EntryImpl(new byte[]{2}, new byte[]{20}, 2, 3),
            new EntryImpl(new byte[]{3}, new byte[]{30}, 3, 4),
    };

    @BeforeEach
    public void setUp() {
        cond1 = cond(new byte[][] {{1}, {2}}, true);
        cond2 = cond(new byte[][] {{3}}, true);
        cond3 = cond(new byte[][] {{5}, {6}}, false);
        cond4 = cond(new byte[][] {{7}}, false);
    }

    @Test
    public void testTrueTrue() {
        var cond = new AndCondition(cond1, cond2);

        assertArrayEquals(ArrayUtils.concat(cond1.keys(), cond2.keys()), cond.keys());
        assertTrue(cond.test(entries));
        verify(cond1, times(1)).test(Arrays.copyOf(entries, 2));
        verify(cond2, times(1)).test(Arrays.copyOfRange(entries, 2, 3));
    }

    @Test
    public void testTrueFalse() {
        var cond = new AndCondition(cond2, cond3);

        assertArrayEquals(ArrayUtils.concat(cond2.keys(), cond3.keys()), cond.keys());
        assertFalse(cond.test(entries));
        verify(cond2, times(1)).test(Arrays.copyOf(entries, 1));
        verify(cond3, times(1)).test(Arrays.copyOfRange(entries, 1, 3));
    }

    @Test
    public void testFalseTrue() {
        var cond = new AndCondition(cond3, cond2);

        assertArrayEquals(ArrayUtils.concat(cond3.keys(), cond2.keys()), cond.keys());
        assertFalse(cond.test(entries));
        verify(cond3, times(1)).test(Arrays.copyOf(entries, 2));
        verify(cond2, times(1)).test(Arrays.copyOfRange(entries, 2, 3));
    }

    @Test
    public void testFalseFalse() {
        var cond = new AndCondition(cond3, cond4);

        assertArrayEquals(ArrayUtils.concat(cond3.keys(), cond4.keys()), cond.keys());
        assertFalse(cond.test(entries));
        verify(cond3, times(1)).test(Arrays.copyOf(entries, 2));
        verify(cond4, times(1)).test(Arrays.copyOfRange(entries, 2, 3));
    }

    private static Condition cond(byte[][] keys, boolean result) {
        var m = mock(Condition.class);

        when(m.keys()).thenReturn(keys);

        when(m.test(any(Entry[].class))).thenReturn(result);

        return m;
    }
}
