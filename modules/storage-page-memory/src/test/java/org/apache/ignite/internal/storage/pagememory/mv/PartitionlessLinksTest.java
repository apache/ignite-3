/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.storage.pagememory.mv;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Test;

class PartitionlessLinksTest {
    @Test
    void removesPartitionIdFromFullLink() {
        // 0xABCD is partitionId
        long fullLink = 0x7733ABCDDEADBEEFL;

        long partitionlessLink = PartitionlessLinks.removePartitionIdFromLink(fullLink);

        assertThat(partitionlessLink, is(0x7733DEADBEEFL));
    }

    @Test
    void addsPartitionId() {
        long partitionlessLink = 0x7733DEADBEEFL;

        long fullLink = PartitionlessLinks.addPartitionIdToPartititionlessLink(partitionlessLink, 0xABCD);

        assertThat(fullLink, is(0x7733ABCDDEADBEEFL));
    }

    @Test
    void removalAndAdditionOfPartitionIdRestoresLink() {
        long fullLink = 0x7733ABCDDEADBEEFL;

        long partitionlessLink = PartitionlessLinks.removePartitionIdFromLink(fullLink);
        long reconstructedLink = PartitionlessLinks.addPartitionIdToPartititionlessLink(partitionlessLink, 0xABCD);

        assertThat(reconstructedLink, is(fullLink));
    }

    @Test
    void extractsHigh16Bits() {
        short high16Bits = PartitionlessLinks.high16Bits(0x7733DEADBEEFL);

        assertThat(high16Bits, is((short) 0x7733));
    }

    @Test
    void extractsLow32Bits() {
        int low32Bits = PartitionlessLinks.low32Bits(0x7733DEADBEEFL);

        assertThat(low32Bits, is(0xDEADBEEF));
    }
}
