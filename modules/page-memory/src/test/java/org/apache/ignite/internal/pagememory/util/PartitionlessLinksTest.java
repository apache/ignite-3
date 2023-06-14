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

package org.apache.ignite.internal.pagememory.util;

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.NULL_LINK;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.link;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.PARTITIONLESS_LINK_SIZE_BYTES;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.readPartitionless;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.writePartitionless;
import static org.apache.ignite.internal.util.GridUnsafe.allocateMemory;
import static org.apache.ignite.internal.util.GridUnsafe.freeMemory;
import static org.apache.ignite.internal.util.GridUnsafe.zeroMemory;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.function.LongConsumer;
import org.junit.jupiter.api.Test;

/**
 * For {@link PartitionlessLinks} testing.
 */
public class PartitionlessLinksTest {
    @Test
    void testPartitionLessLink() {
        withAllocateMemory(512, addr -> {
            int partitionId = 1;

            int offset = 0;

            assertEquals(NULL_LINK, readPartitionless(partitionId, addr, offset));

            long link = link(pageId(partitionId, FLAG_DATA, 1), 2);

            assertEquals(PARTITIONLESS_LINK_SIZE_BYTES, writePartitionless(addr, link));

            assertEquals(link, readPartitionless(partitionId, addr, offset));
        });
    }

    @Test
    void testPartitionLessPageId() {
        withAllocateMemory(512, addr -> {
            int partitionId = 1;

            int offset = 0;

            assertEquals(NULL_LINK, readPartitionless(partitionId, addr, offset));

            long pageId = pageId(partitionId, FLAG_AUX, 2);

            assertEquals(PARTITIONLESS_LINK_SIZE_BYTES, writePartitionless(addr + offset, pageId));

            assertEquals(pageId, readPartitionless(partitionId, addr, offset));
        });
    }

    private static void withAllocateMemory(int size, LongConsumer addrConsumer) {
        long addr = allocateMemory(size);

        try {
            zeroMemory(addr, size);

            addrConsumer.accept(addr);
        } finally {
            freeMemory(addr);
        }
    }
}
