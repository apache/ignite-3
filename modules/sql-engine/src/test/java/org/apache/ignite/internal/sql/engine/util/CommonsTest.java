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
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.junit.jupiter.api.Test;

/**
 * Tests for utility functions defined in {@link Commons}.
 */
public class CommonsTest {

    @Test
    public void testMapBitSet() {
        Mapping mapping = Mappings.create(MappingType.PARTIAL_FUNCTION, 7, 7);
        mapping.set(0, 1);
        mapping.set(4, 6);
        mapping.set(1, 4);

        ImmutableBitSet bitSet = ImmutableBitSet.of(1, 0, 4);

        ImmutableBitSet actual = Commons.mapBitSet(bitSet, mapping);
        assertEquals(ImmutableBitSet.of(1, 4, 6), actual);
    }

    @Test
    public void testMapEmptyBitSet() {
        Mapping mapping = Mappings.createIdentity(1);
        ImmutableBitSet bitSet = ImmutableBitSet.of();

        ImmutableBitSet actual = Commons.mapBitSet(bitSet, mapping);
        assertEquals(ImmutableBitSet.of(), actual);
    }
}
