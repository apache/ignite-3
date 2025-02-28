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

import org.apache.ignite.internal.network.NetworkMessage;
import org.junit.jupiter.api.Test;

/**
 * Tests that persisted enum ordinals have not been accidentally changed by a developer.
 */
class ConditionTypeTest {
    /** Checks that the transferable ID does not change, since the enum will be transferred in the {@link NetworkMessage}. */
    @Test
    void testTransferableId() {
        assertEquals(0, ConditionType.REV_EQUAL.transferableId());
        assertEquals(1, ConditionType.REV_NOT_EQUAL.transferableId());
        assertEquals(2, ConditionType.REV_GREATER.transferableId());
        assertEquals(3, ConditionType.REV_LESS.transferableId());
        assertEquals(4, ConditionType.REV_LESS_OR_EQUAL.transferableId());
        assertEquals(5, ConditionType.REV_GREATER_OR_EQUAL.transferableId());
        assertEquals(6, ConditionType.VAL_EQUAL.transferableId());
        assertEquals(7, ConditionType.VAL_NOT_EQUAL.transferableId());
        assertEquals(8, ConditionType.VAL_GREATER.transferableId());
        assertEquals(9, ConditionType.VAL_LESS.transferableId());
        assertEquals(10, ConditionType.VAL_LESS_OR_EQUAL.transferableId());
        assertEquals(11, ConditionType.VAL_GREATER_OR_EQUAL.transferableId());
        assertEquals(12, ConditionType.KEY_EXISTS.transferableId());
        assertEquals(13, ConditionType.KEY_NOT_EXISTS.transferableId());
        assertEquals(14, ConditionType.TOMBSTONE.transferableId());
        assertEquals(15, ConditionType.NOT_TOMBSTONE.transferableId());
    }
}
