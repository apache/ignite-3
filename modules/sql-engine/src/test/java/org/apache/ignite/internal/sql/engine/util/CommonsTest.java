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

import org.apache.calcite.util.ImmutableBitSet;
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

    private static void expectMapped(Mapping mapping, ImmutableBitSet bitSet, ImmutableBitSet expected) {
        assertEquals(expected, Mappings.apply(mapping, bitSet), "direct mapping");

        Mapping inverseMapping = mapping.inverse();
        assertEquals(bitSet, Mappings.apply(inverseMapping, expected), "inverse mapping");
    }
}
