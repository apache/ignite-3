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

package org.apache.ignite.internal.pagememory.persistence.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.ignite.internal.pagememory.persistence.PartitionMeta;
import org.junit.jupiter.api.Test;

/**
 * For {@link PartitionFilePageStore} testing.
 */
public class PartitionFilePageStoreTest {
    @Test
    void testUpdateMetaPageCount() {
        FilePageStore filePageStore = mock(FilePageStore.class);

        PartitionFilePageStore partitionFilePageStore = new PartitionFilePageStore(filePageStore, new PartitionMeta(0, 0, 0));

        when(filePageStore.pages()).thenReturn(100500);

        assertEquals(0, partitionFilePageStore.meta().pageCount());

        partitionFilePageStore.updateMetaPageCount();

        assertEquals(100500, partitionFilePageStore.meta().pageCount());
    }
}
