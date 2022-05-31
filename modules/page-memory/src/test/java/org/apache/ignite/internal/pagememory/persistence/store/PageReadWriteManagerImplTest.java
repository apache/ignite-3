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

import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * For {@link PageReadWriteManagerImpl} testing.
 */
public class PageReadWriteManagerImplTest {
    private FilePageStore filePageStore;

    private FilePageStoreManager filePageStoreManager;

    private PageReadWriteManagerImpl pageReadWriteManager;

    @BeforeEach
    void setUp() throws Exception {
        filePageStore = mock(FilePageStore.class);

        filePageStoreManager = mock(FilePageStoreManager.class);

        when(filePageStoreManager.getStore(0, 0)).thenReturn(filePageStore);

        pageReadWriteManager = new PageReadWriteManagerImpl(filePageStoreManager);
    }

    @Test
    void testRead() throws Exception {
        long pageId = pageId(0, (byte) 0, 0);

        ByteBuffer pageBuffer = mock(ByteBuffer.class);

        pageReadWriteManager.read(0, pageId, pageBuffer, true);

        verify(filePageStore, times(1)).read(pageId, pageBuffer, true);
    }

    @Test
    void testWrite() throws Exception {
        long pageId = pageId(0, (byte) 0, 0);

        ByteBuffer pageBuffer = mock(ByteBuffer.class);

        assertSame(filePageStore, pageReadWriteManager.write(0, pageId, pageBuffer, 0, true));

        verify(filePageStore, times(1)).write(pageId, pageBuffer, 0, true);
    }

    @Test
    void testAllocatePage() throws Exception {
        assertEquals(
                pageId(0, (byte) 1, 0),
                pageReadWriteManager.allocatePage(0, 0, (byte) 1)
        );

        verify(filePageStore, times(1)).allocatePage();
    }
}
