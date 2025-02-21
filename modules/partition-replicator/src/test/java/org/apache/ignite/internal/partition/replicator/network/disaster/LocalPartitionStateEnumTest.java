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

import org.apache.ignite.internal.network.NetworkMessage;
import org.junit.jupiter.api.Test;

/** For {@link LocalPartitionStateEnum} testing. */
public class LocalPartitionStateEnumTest {
    /** Checks that the transferable ID does not change, since the enum will be transferred in the {@link NetworkMessage}. */
    @Test
    void testTransferableId() {
        assertEquals(0, LocalPartitionStateEnum.UNAVAILABLE.transferableId());
        assertEquals(1, LocalPartitionStateEnum.HEALTHY.transferableId());
        assertEquals(2, LocalPartitionStateEnum.INITIALIZING.transferableId());
        assertEquals(3, LocalPartitionStateEnum.INSTALLING_SNAPSHOT.transferableId());
        assertEquals(4, LocalPartitionStateEnum.CATCHING_UP.transferableId());
        assertEquals(5, LocalPartitionStateEnum.BROKEN.transferableId());
    }
}
