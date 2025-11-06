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

package org.apache.ignite.internal.sql.engine.exec.fsm;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import org.junit.jupiter.api.Test;

/**
 * DdlBatchingHelper self-test.
 */
public class DdlBatchingHelperTest {
    @Test
    void testDdlBatchingHelper() {
        EnumSet<DdlBatchGroup> allExceptOther = EnumSet.complementOf(EnumSet.of(DdlBatchGroup.OTHER));

        assert !allExceptOther.isEmpty();

        // Any group is compatible with itself (except OTHER).
        for (DdlBatchGroup group : allExceptOther) {
            assertTrue(DdlBatchingHelper.isCompatible(group, group), group.toString());
        }

        // DROP can be followed by any other group (incl. OTHER).
        for (DdlBatchGroup group : DdlBatchGroup.values()) {
            assertTrue(DdlBatchingHelper.isCompatible(DdlBatchGroup.DROP, group), group.toString());
        }

        // Other incompatible cases.
        for (DdlBatchGroup batchGroup : EnumSet.complementOf(EnumSet.of(DdlBatchGroup.DROP))) {
            for (DdlBatchGroup group : EnumSet.complementOf(EnumSet.of(batchGroup))) {
                assertFalse(DdlBatchingHelper.isCompatible(batchGroup, group), batchGroup + " vs " + group);
            }
        }
    }
}
