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
class OperationTypeTest {
    private static Stream<Arguments> operationIds() {
        return Stream.of(
                arguments(OperationType.NO_OP, 0),
                arguments(OperationType.PUT, 1),
                arguments(OperationType.REMOVE, 2)
        );
    }

    @ParameterizedTest
    @MethodSource("operationIds")
    void testId(OperationType operationType, int expectedId) {
        assertEquals(expectedId, operationType.id());
    }

    /** Checks that the ID does not change, since the enum will be transferred in the {@link NetworkMessage}. */
    @ParameterizedTest
    @MethodSource("operationIds")
    void testFromId(OperationType expectedOperationType, int id) {
        assertEquals(expectedOperationType, OperationType.fromId(id));

    }

    @Test
    void testFromIdThrows() {
        assertThrows(IllegalArgumentException.class, () -> OperationType.fromId(-1));
        assertThrows(IllegalArgumentException.class, () -> OperationType.fromId(3));
    }
}
