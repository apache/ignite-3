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

import org.apache.ignite.internal.network.NetworkMessage;
import org.junit.jupiter.api.Test;

/** For {@link LocalPartitionStateEnum} testing. */
public class LocalPartitionStateEnumTest {
    /** Checks that the ordinal does not change, since the enum will be transferred in the {@link NetworkMessage}. */
    @Test
    void testFromOrdinal() {
        assertEquals(LocalPartitionStateEnum.UNAVAILABLE, LocalPartitionStateEnum.fromOrdinal(0));

        assertEquals(LocalPartitionStateEnum.HEALTHY, LocalPartitionStateEnum.fromOrdinal(1));

        assertEquals(LocalPartitionStateEnum.INITIALIZING, LocalPartitionStateEnum.fromOrdinal(2));

        assertEquals(LocalPartitionStateEnum.INSTALLING_SNAPSHOT, LocalPartitionStateEnum.fromOrdinal(3));

        assertEquals(LocalPartitionStateEnum.CATCHING_UP, LocalPartitionStateEnum.fromOrdinal(4));

        assertEquals(LocalPartitionStateEnum.BROKEN, LocalPartitionStateEnum.fromOrdinal(5));

        assertThrows(IllegalArgumentException.class, () -> LocalPartitionStateEnum.fromOrdinal(-1));
        assertThrows(IllegalArgumentException.class, () -> LocalPartitionStateEnum.fromOrdinal(6));
    }
}
