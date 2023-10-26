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

import org.apache.ignite.internal.metastorage.dsl.ConditionType;
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
}
