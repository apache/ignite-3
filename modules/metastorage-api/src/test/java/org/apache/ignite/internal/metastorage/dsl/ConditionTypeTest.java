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
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;
import org.apache.ignite.internal.network.NetworkMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests that persisted enum IDs have not been accidentally changed by a developer.
 */
class ConditionTypeTest {
    private static Stream<Arguments> conditionTypeIds() {
        return Stream.of(
                arguments(ConditionType.REV_EQUAL, 0),
                arguments(ConditionType.REV_NOT_EQUAL, 1),
                arguments(ConditionType.REV_GREATER, 2),
                arguments(ConditionType.REV_LESS, 3),
                arguments(ConditionType.REV_LESS_OR_EQUAL, 4),
                arguments(ConditionType.REV_GREATER_OR_EQUAL, 5),
                arguments(ConditionType.VAL_EQUAL, 6),
                arguments(ConditionType.VAL_NOT_EQUAL, 7),
                arguments(ConditionType.VAL_GREATER, 8),
                arguments(ConditionType.VAL_LESS, 9),
                arguments(ConditionType.VAL_LESS_OR_EQUAL, 10),
                arguments(ConditionType.VAL_GREATER_OR_EQUAL, 11),
                arguments(ConditionType.KEY_EXISTS, 12),
                arguments(ConditionType.KEY_NOT_EXISTS, 13),
                arguments(ConditionType.TOMBSTONE, 14),
                arguments(ConditionType.NOT_TOMBSTONE, 15)
        );
    }

    @ParameterizedTest
    @MethodSource("conditionTypeIds")
    void testId(ConditionType conditionType, int expectedId) {
        assertEquals(expectedId, conditionType.id());
    }

    /** Checks that the ID does not change, since the enum will be transferred in the {@link NetworkMessage}. */
    @ParameterizedTest
    @MethodSource("conditionTypeIds")
    void testFromId(ConditionType expectedEnumEntry, int id) {
        assertEquals(expectedEnumEntry, ConditionType.fromId(id));

    }

    @Test
    void testFromIdThrows() {
        assertThrows(IllegalArgumentException.class, () -> ConditionType.fromId(-1));
        assertThrows(IllegalArgumentException.class, () -> ConditionType.fromId(16));
    }
}
