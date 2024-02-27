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

package org.apache.ignite.internal.lang;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Test for logger helper.
 */
public class LoggerHelperTest {
    /** Message parameter 1. */
    Integer i1 = 1;

    /** Message parameter 2. */
    Integer i2 = 2;

    /** Message parameter 3. */
    Integer i3 = 3;

    /** Array message parameter 0. */
    Integer[] ia0 = new Integer[]{i1, i2, i3};

    /** Array message parameter 1. */
    Integer[] ia1 = new Integer[]{10, 20, 30};

    /** Variable for the result message. */
    String result;

    /**
     * Tests {@code null} message pattern.
     */
    @Test
    public void testNull() {
        result = IgniteStringFormatter.format(null, i1);
        assertEquals(null, result);
    }

    /**
     * Tests parameters are {@code null}'s.
     */
    @Test
    public void nullParametersShouldBeHandledWithoutBarfing() {
        result = IgniteStringFormatter.format("Value is {}.", new Object[]{null});
        assertEquals("Value is null.", result);

        result = IgniteStringFormatter.format("Val1 is {}, val2 is {}.", null, null);
        assertEquals("Val1 is null, val2 is null.", result);

        result = IgniteStringFormatter.format("Val1 is {}, val2 is {}.", i1, null);
        assertEquals("Val1 is 1, val2 is null.", result);

        result = IgniteStringFormatter.format("Val1 is {}, val2 is {}.", null, i2);
        assertEquals("Val1 is null, val2 is 2.", result);

        result = IgniteStringFormatter.format("Val1 is {}, val2 is {}, val3 is {}", new Integer[]{null, null, null});
        assertEquals("Val1 is null, val2 is null, val3 is null", result);

        result = IgniteStringFormatter.format("Val1 is {}, val2 is {}, val3 is {}", new Integer[]{null, i2, i3});
        assertEquals("Val1 is null, val2 is 2, val3 is 3", result);

        result = IgniteStringFormatter.format("Val1 is {}, val2 is {}, val3 is {}", new Integer[]{null, null, i3});
        assertEquals("Val1 is null, val2 is null, val3 is 3", result);
    }

    /**
     * Tests the result string when parameter is only one.
     */
    @Test
    public void verifyOneParameterIsHandledCorrectly() {
        result = IgniteStringFormatter.format("Value is {}.", i3);
        assertEquals("Value is 3.", result);

        result = IgniteStringFormatter.format("Value is {", i3);
        assertEquals("Value is {", result);

        result = IgniteStringFormatter.format("{} is larger than 2.", i3);
        assertEquals("3 is larger than 2.", result);

        result = IgniteStringFormatter.format("No subst", i3);
        assertEquals("No subst", result);

        result = IgniteStringFormatter.format("Incorrect {subst", i3);
        assertEquals("Incorrect {subst", result);

        result = IgniteStringFormatter.format("Value is {bla} {}", i3);
        assertEquals("Value is {bla} 3", result);

        result = IgniteStringFormatter.format("Escaped \\{} subst", i3);
        assertEquals("Escaped {} subst", result);

        result = IgniteStringFormatter.format("{Escaped", i3);
        assertEquals("{Escaped", result);

        result = IgniteStringFormatter.format("\\{}Escaped", i3);
        assertEquals("{}Escaped", result);

        result = IgniteStringFormatter.format("File name is {{}}.", "App folder.zip");
        assertEquals("File name is {App folder.zip}.", result);

        // escaping the escape character
        result = IgniteStringFormatter.format("File name is C:\\\\{}.", "App folder.zip");
        assertEquals("File name is C:\\App folder.zip.", result);
    }

    /**
     * Tests the result string when two parameters.
     */
    @Test
    public void testTwoParameters() {
        result = IgniteStringFormatter.format("Value {} is smaller than {}.", i1, i2);
        assertEquals("Value 1 is smaller than 2.", result);

        result = IgniteStringFormatter.format("Value {} is smaller than {}", i1, i2);
        assertEquals("Value 1 is smaller than 2", result);

        result = IgniteStringFormatter.format("{}{}", i1, i2);
        assertEquals("12", result);

        result = IgniteStringFormatter.format("Val1={}, Val2={", i1, i2);
        assertEquals("Val1=1, Val2={", result);

        result = IgniteStringFormatter.format("Value {} is smaller than \\{}", i1, i2);
        assertEquals("Value 1 is smaller than {}", result);

        result = IgniteStringFormatter.format("Value {} is smaller than \\{} tail", i1, i2);
        assertEquals("Value 1 is smaller than {} tail", result);

        result = IgniteStringFormatter.format("Value {} is smaller than \\{", i1, i2);
        assertEquals("Value 1 is smaller than \\{", result);

        result = IgniteStringFormatter.format("Value {} is smaller than {tail", i1, i2);
        assertEquals("Value 1 is smaller than {tail", result);

        result = IgniteStringFormatter.format("Value \\{} is smaller than {}", i1, i2);
        assertEquals("Value {} is smaller than 1", result);
    }

    @Test
    public void testExceptionIn_toString() {
        Object o = new Object() {
            @Override
            public String toString() {
                throw new IllegalStateException("a");
            }
        };
        result = IgniteStringFormatter.format("Troublesome object {}", o);
        assertEquals("Troublesome object Failed toString() invocation on an object of type [cls=" + o.getClass().getName()
                + ", errMsg=java.lang.IllegalStateException, errMsg=a]", result);
    }

    @Test
    public void testNullArray() {
        String msg0 = "msg0";
        String msg1 = "msg1 {}";
        final String msg2 = "msg2 {} {}";
        final String msg3 = "msg3 {} {} {}";

        Object[] args = null;

        result = IgniteStringFormatter.format(msg0, args);
        assertEquals(msg0, result);

        result = IgniteStringFormatter.format(msg1, args);
        assertEquals(msg1, result);

        result = IgniteStringFormatter.format(msg2, args);
        assertEquals(msg2, result);

        result = IgniteStringFormatter.format(msg3, args);
        assertEquals(msg3, result);
    }

    /**
     * Tests the case when the parameters are supplied in a single array.
     */
    @Test
    public void testArrayFormat() {
        result = IgniteStringFormatter.format("Value {} is smaller than {} and {}.", ia0);
        assertEquals("Value 1 is smaller than 2 and 3.", result);

        result = IgniteStringFormatter.format("{}{}{}", ia0);
        assertEquals("123", result);

        result = IgniteStringFormatter.format("Value {} is smaller than {}.", ia0);
        assertEquals("Value 1 is smaller than 2.", result);

        result = IgniteStringFormatter.format("Value {} is smaller than {}", ia0);
        assertEquals("Value 1 is smaller than 2", result);

        result = IgniteStringFormatter.format("Val={}, {, Val={}", ia0);
        assertEquals("Val=1, {, Val=2", result);

        result = IgniteStringFormatter.format("Val={}, {, Val={}", ia0);
        assertEquals("Val=1, {, Val=2", result);

        result = IgniteStringFormatter.format("Val1={}, Val2={", ia0);
        assertEquals("Val1=1, Val2={", result);
    }

    @Test
    public void testArrayValues() {
        Integer p0 = i1;
        Integer[] p1 = new Integer[]{i2, i3};

        result = IgniteStringFormatter.format("{}{}", p0, p1);
        assertEquals("1[2, 3]", result);

        // Integer[]
        result = IgniteStringFormatter.format("{}{}", new Object[]{"a", p1});
        assertEquals("a[2, 3]", result);

        // byte[]
        result = IgniteStringFormatter.format("{}{}", new Object[]{"a", new byte[]{1, 2}});
        assertEquals("a[1, 2]", result);

        // int[]
        result = IgniteStringFormatter.format("{}{}", new Object[]{"a", new int[]{1, 2}});
        assertEquals("a[1, 2]", result);

        // float[]
        result = IgniteStringFormatter.format("{}{}", new Object[]{"a", new float[]{1, 2}});
        assertEquals("a[1.0, 2.0]", result);

        // double[]
        result = IgniteStringFormatter.format("{}{}", new Object[]{"a", new double[]{1, 2}});
        assertEquals("a[1.0, 2.0]", result);
    }

    @Test
    public void testMultiDimensionalArrayValues() {
        Integer[][] multiIntegerA = new Integer[][]{ia0, ia1};
        result = IgniteStringFormatter.format("{}{}", new Object[]{"a", multiIntegerA});
        assertEquals("a[[1, 2, 3], [10, 20, 30]]", result);

        int[][] multiIntA = new int[][]{{1, 2}, {10, 20}};
        result = IgniteStringFormatter.format("{}{}", new Object[]{"a", multiIntA});
        assertEquals("a[[1, 2], [10, 20]]", result);

        float[][] multiFloatA = new float[][]{{1, 2}, {10, 20}};
        result = IgniteStringFormatter.format("{}{}", new Object[]{"a", multiFloatA});
        assertEquals("a[[1.0, 2.0], [10.0, 20.0]]", result);

        Object[][] multiOa = new Object[][]{ia0, ia1};
        result = IgniteStringFormatter.format("{}{}", new Object[]{"a", multiOa});
        assertEquals("a[[1, 2, 3], [10, 20, 30]]", result);

        Object[][][] multiOa3 = new Object[][][]{multiOa, multiOa};
        result = IgniteStringFormatter.format("{}{}", new Object[]{"a", multiOa3});
        assertEquals("a[[[1, 2, 3], [10, 20, 30]], [[1, 2, 3], [10, 20, 30]]]", result);
    }

    @Test
    public void testCyclicArrays() {
        Object[] cyclicA = new Object[1];
        cyclicA[0] = cyclicA;
        assertEquals("[[...]]", IgniteStringFormatter.format("{}", cyclicA));

        Object[] a = new Object[2];
        a[0] = i1;
        Object[] c = new Object[]{i3, a};
        Object[] b = new Object[]{i2, c};
        a[1] = b;
        assertEquals("1[2, [3, [1, [...]]]]", IgniteStringFormatter.format("{}{}", a));

    }
}
