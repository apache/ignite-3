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

package org.apache.ignite.internal.partition.replicator.network.disaster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;
import org.apache.ignite.internal.network.NetworkMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** For {@link LocalPartitionStateEnum} testing. */
public class LocalPartitionStateEnumTest {
    private static Stream<Arguments> conditionTypeIds() {
        return Stream.of(
                arguments(LocalPartitionStateEnum.UNAVAILABLE, 0),
                arguments(LocalPartitionStateEnum.HEALTHY, 1),
                arguments(LocalPartitionStateEnum.INITIALIZING, 2),
                arguments(LocalPartitionStateEnum.INSTALLING_SNAPSHOT, 3),
                arguments(LocalPartitionStateEnum.CATCHING_UP, 4),
                arguments(LocalPartitionStateEnum.BROKEN, 5)
        );
    }

    @ParameterizedTest
    @MethodSource("conditionTypeIds")
    void testId(LocalPartitionStateEnum conditionType, int expectedId) {
        assertEquals(expectedId, conditionType.id());
    }

    /** Checks that the ID does not change, since the enum will be transferred in the {@link NetworkMessage}. */
    @ParameterizedTest
    @MethodSource("conditionTypeIds")
    void testFromId(LocalPartitionStateEnum expectedEnumEntry, int id) {
        assertEquals(expectedEnumEntry, LocalPartitionStateEnum.fromId(id));

    }

    @Test
    void testFromIdThrows() {
        assertThrows(IllegalArgumentException.class, () -> LocalPartitionStateEnum.fromId(-1));
        assertThrows(IllegalArgumentException.class, () -> LocalPartitionStateEnum.fromId(6));
    }
}
