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

package org.apache.ignite.internal.sql.engine.prepare;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.lang.NullableValue;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DynamicParameters}.
 */
public class DynamicParametersTest extends BaseIgniteAbstractTest {

    @Test
    public void testEmpty() {
        DynamicParameters params = DynamicParameters.builder().build();

        expectParams(params, new Object[0]);
    }

    @Test
    public void testFullySet() {
        DynamicParameters params = DynamicParameters.builder()
                .set(0, "0")
                .set(1, "1")
                .build();

        Map<Integer, Object> values = new HashMap<>();
        values.put(0, "0");
        values.put(1, "1");

        expectParams(params, new Object[]{"0", "1"});
    }

    @Test
    public void testNullValues() {
        DynamicParameters params = DynamicParameters.builder()
                .set(0, "0")
                .set(1, null)
                .set(2, "2")
                .build();

        expectParams(params, new Object[]{"0", null, "2"});
    }

    @Test
    public void testOf() {
        expectParams(DynamicParameters.of(), new Object[0]);
        expectParams(DynamicParameters.of("0", "1"), new Object[]{"0", "1"});
    }

    @Test
    public void testToArrayFailsWhenSomeParametersAreNotSet() {
        DynamicParameters params = DynamicParameters.builder()
                .set(1, "a")
                .build();

        IllegalStateException err = assertThrows(IllegalStateException.class, params::toArray);
        assertThat(err.getMessage(), containsString("Not all parameters have been specified"));
        assertThat(err.getMessage(), containsString("{0: <none>, 1: a}"));
    }

    @Test
    public void testToMapFailsWhenSomeParametersAreNotSet() {
        DynamicParameters params = DynamicParameters.builder()
                .set(1, "a")
                .build();

        IllegalStateException err = assertThrows(IllegalStateException.class, params::toMap);
        assertThat(err.getMessage(), containsString("Not all parameters have been specified"));
        assertThat(err.getMessage(), containsString("{0: <none>, 1: a}"));
    }

    @Test
    public void testPartiallySet1() {
        DynamicParameters params = DynamicParameters.builder()
                .set(1, "1")
                .set(2, "2")
                .build();

        Map<Integer, Object> values = new HashMap<>();
        values.put(0, NotSet.VALUE);
        values.put(1, "1");
        values.put(2, "2");

        expectParams(params, new Object[]{NotSet.VALUE, "1", "2"});
    }

    @Test
    public void testPartiallySet1_1() {
        DynamicParameters params = DynamicParameters.builder()
                .set(2, "2")
                .build();

        Map<Integer, Object> values = new HashMap<>();
        values.put(0, NotSet.VALUE);
        values.put(1, NotSet.VALUE);
        values.put(2, "2");

        expectParams(params, new Object[]{NotSet.VALUE, NotSet.VALUE, "2"});
    }

    @Test
    public void testPartiallySet2() {
        DynamicParameters params = DynamicParameters.builder()
                .set(0, "0")
                .set(1, "1")
                .set(3, "3")
                .build();

        Map<Integer, Object> values = new HashMap<>();
        values.put(0, "0");
        values.put(1, "1");
        values.put(2, NotSet.VALUE);
        values.put(3, "3");

        expectParams(params, new Object[]{"0", "1", NotSet.VALUE, "3"});
    }

    @Test
    public void testPartiallySet2_1() {
        DynamicParameters params = DynamicParameters.builder()
                .set(0, "0")
                .set(1, "1")
                .set(4, "4")
                .build();

        expectParams(params, new Object[]{"0", "1", NotSet.VALUE, NotSet.VALUE, "4"});
    }

    @Test
    public void testToArray() {
        assertArrayEquals(new Object[0], DynamicParameters.builder().build().toArray());
        assertArrayEquals(new Object[0], DynamicParameters.empty().toArray());

        DynamicParameters params = DynamicParameters.builder().set(0, "0").set(1, null).set(2, "2").build();
        assertArrayEquals(new Object[]{"0", null, "2"}, params.toArray(), params.toString());
    }

    @Test
    public void testToMap() {
        assertEquals(Map.of(), DynamicParameters.builder().build().toMap());
        assertEquals(Map.of(), DynamicParameters.empty().toMap());

        DynamicParameters params = DynamicParameters.builder().set(0, "0").set(1, null).set(2, "2").build();

        Map<String, Object> map = new HashMap<>();
        map.put("?0", "0");
        map.put("?1", null);
        map.put("?2", "2");

        assertEquals(map, params.toMap(), params.toString());
    }

    private static void expectParams(DynamicParameters params, Object[] expected) {
        Object[] actual = new Object[expected.length];

        for (int i = 0; i < params.size(); i++) {
            NullableValue<Object> value = params.value(i);
            actual[i] = value != null ? value.get() : NotSet.VALUE;
        }

        assertArrayEquals(expected, actual, params.toString());
    }

    private enum NotSet {
        VALUE;

        @Override
        public String toString() {
            return "<NotSet>";
        }
    }
}
