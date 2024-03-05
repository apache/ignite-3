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

package org.apache.ignite.internal.sql.engine.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.junit.jupiter.api.Test;

/**
 * Tests for utility functions defined in {@link Commons}.
 */
public class CommonsTest {

    @Test
    public void testTrimmingMapping() {
        ImmutableBitSet requiredElements = ImmutableBitSet.of(1, 4, 3, 7);

        Mapping mapping = Commons.trimmingMapping(requiredElements.length(), requiredElements);
        expectMapped(mapping, ImmutableBitSet.of(1, 7), ImmutableBitSet.of(0, 3));
        expectMapped(mapping, ImmutableBitSet.of(3), ImmutableBitSet.of(1));
        expectMapped(mapping, ImmutableBitSet.of(1, 4, 3, 7), ImmutableBitSet.of(0, 1, 2, 3));
    }

    @Test
    public void testProjectionMapping() {
        assertProjectionMapping(
                ImmutableIntList.of(1, 2, 3, 4), // source
                ImmutableIntList.of(3, 2), // projection
                ImmutableIntList.of(4, 3) // expected result
        );

        assertProjectionMapping(
                ImmutableIntList.of(1, 2, 3, 4), // source
                ImmutableIntList.of(3, 0), // projection
                ImmutableIntList.of(4, 1) // expected result
        );

        assertProjectionMapping(
                ImmutableIntList.of(1, 2, 3, 4), // source
                ImmutableIntList.of(1), // projection
                ImmutableIntList.of(2) // expected result
        );
    }

    @Test
    public void testArrayToMap() {
        assertEquals(Map.of(), Commons.arrayToMap(null));

        HashMap<Integer, Object> vals = new HashMap<>();
        vals.put(0, 1);
        vals.put(1, null);
        vals.put(2, "3");
        assertEquals(vals, Commons.arrayToMap(new Object[]{1, null, "3"}));
    }

    @Test
    public void testMakeRelTypeName() {
        assertEquals("N", Commons.makeRelTypeName(IgniteN.class));
        assertEquals("Ignite", Commons.makeRelTypeName(Ignite.class));
        assertEquals("Ignit", Commons.makeRelTypeName(Ignit.class));
        assertEquals("NonIgniteRel", Commons.makeRelTypeName(NonIgniteRel.class));
        assertEquals(Commons.class.getSimpleName(), Commons.makeRelTypeName(Commons.class));
    }

    private static void expectMapped(Mapping mapping, ImmutableBitSet bitSet, ImmutableBitSet expected) {
        assertEquals(expected, Mappings.apply(mapping, bitSet), "direct mapping");

        Mapping inverseMapping = mapping.inverse();
        assertEquals(bitSet, Mappings.apply(inverseMapping, expected), "inverse mapping");
    }

    private static void assertProjectionMapping(ImmutableIntList source, ImmutableIntList projection, ImmutableIntList expected) {
        Mapping mapping = Commons.projectedMapping(source.size(), projection);

        assertEquals(expected, Mappings.apply(mapping, source));
    }

    private static class IgniteN {}

    private static class Ignite {}

    private static class Ignit {}

    private static class NonIgniteRel {}
}
