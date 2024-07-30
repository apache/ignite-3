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

package org.apache.ignite.internal.metastorage.dsl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.ignite.internal.network.NetworkMessage;
import org.junit.jupiter.api.Test;

/**
 * Tests that persisted enum ordinals have not been accidentally changed by a developer.
 */
class ConditionTypeTest {
    @Test
    void testOrdinal() {
        assertEquals(0, ConditionType.REV_EQUAL.ordinal());

        assertEquals(1, ConditionType.REV_NOT_EQUAL.ordinal());

        assertEquals(2, ConditionType.REV_GREATER.ordinal());

        assertEquals(3, ConditionType.REV_LESS.ordinal());

        assertEquals(4, ConditionType.REV_LESS_OR_EQUAL.ordinal());

        assertEquals(5, ConditionType.REV_GREATER_OR_EQUAL.ordinal());

        assertEquals(6, ConditionType.VAL_EQUAL.ordinal());

        assertEquals(7, ConditionType.VAL_NOT_EQUAL.ordinal());

        assertEquals(8, ConditionType.VAL_GREATER.ordinal());

        assertEquals(9, ConditionType.VAL_LESS.ordinal());

        assertEquals(10, ConditionType.VAL_LESS_OR_EQUAL.ordinal());

        assertEquals(11, ConditionType.VAL_GREATER_OR_EQUAL.ordinal());

        assertEquals(12, ConditionType.KEY_EXISTS.ordinal());

        assertEquals(13, ConditionType.KEY_NOT_EXISTS.ordinal());

        assertEquals(14, ConditionType.TOMBSTONE.ordinal());

        assertEquals(15, ConditionType.NOT_TOMBSTONE.ordinal());
    }

    /** Checks that the ordinal does not change, since the enum will be transfer in the {@link NetworkMessage}. */
    @Test
    void testFromOrdinal() {
        assertEquals(ConditionType.REV_EQUAL, ConditionType.fromOrdinal(0));

        assertEquals(ConditionType.REV_NOT_EQUAL, ConditionType.fromOrdinal(1));

        assertEquals(ConditionType.REV_GREATER, ConditionType.fromOrdinal(2));

        assertEquals(ConditionType.REV_LESS, ConditionType.fromOrdinal(3));

        assertEquals(ConditionType.REV_LESS_OR_EQUAL, ConditionType.fromOrdinal(4));

        assertEquals(ConditionType.REV_GREATER_OR_EQUAL, ConditionType.fromOrdinal(5));

        assertEquals(ConditionType.VAL_EQUAL, ConditionType.fromOrdinal(6));

        assertEquals(ConditionType.VAL_NOT_EQUAL, ConditionType.fromOrdinal(7));

        assertEquals(ConditionType.VAL_GREATER, ConditionType.fromOrdinal(8));

        assertEquals(ConditionType.VAL_LESS, ConditionType.fromOrdinal(9));

        assertEquals(ConditionType.VAL_LESS_OR_EQUAL, ConditionType.fromOrdinal(10));

        assertEquals(ConditionType.VAL_GREATER_OR_EQUAL, ConditionType.fromOrdinal(11));

        assertEquals(ConditionType.KEY_EXISTS, ConditionType.fromOrdinal(12));

        assertEquals(ConditionType.KEY_NOT_EXISTS, ConditionType.fromOrdinal(13));

        assertEquals(ConditionType.TOMBSTONE, ConditionType.fromOrdinal(14));

        assertEquals(ConditionType.NOT_TOMBSTONE, ConditionType.fromOrdinal(15));

        assertThrows(IllegalArgumentException.class, () -> ConditionType.fromOrdinal(-1));
        assertThrows(IllegalArgumentException.class, () -> ConditionType.fromOrdinal(16));
    }
}
