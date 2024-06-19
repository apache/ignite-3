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

package org.apache.ignite.internal.pagememory;

import static org.apache.ignite.internal.util.Constants.KiB;
import static org.apache.ignite.internal.util.Constants.MiB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.TestPageIoModule.TestPageIo;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/**
 * Abstract class for non-load testing of {@link PageMemory} implementations.
 */
public abstract class AbstractPageMemoryNoLoadSelfTest extends BaseIgniteAbstractTest {
    protected static final int PAGE_SIZE = 8 * KiB;

    protected static final int MAX_MEMORY_SIZE = 10 * MiB;

    private static final PageIo PAGE_IO = new TestPageIo();

    protected static final int GRP_ID = -1;

    protected static final int PARTITION_ID = 1;

    @Test
    public void testPageTearingInner() throws Exception {
        PageMemory mem = memory();

        mem.start();

        try {
            FullPageId fullId1 = allocatePage(mem);
            FullPageId fullId2 = allocatePage(mem);

            long page1 = mem.acquirePage(fullId1.groupId(), fullId1.pageId());

            try {
                long page2 = mem.acquirePage(fullId2.groupId(), fullId2.pageId());

                log.info("Allocated pages [page1Id=" + fullId1.pageId() + ", page1=" + page1
                        + ", page2Id=" + fullId2.pageId() + ", page2=" + page2 + ']');

                try {
                    writePage(mem, fullId1, page1, 1);
                    writePage(mem, fullId2, page2, 2);

                    readPage(mem, fullId1.pageId(), page1, 1);
                    readPage(mem, fullId2.pageId(), page2, 2);

                    // Check read after read.
                    readPage(mem, fullId1.pageId(), page1, 1);
                    readPage(mem, fullId2.pageId(), page2, 2);
                } finally {
                    mem.releasePage(fullId2.groupId(), fullId2.pageId(), page2);
                }
            } finally {
                mem.releasePage(fullId1.groupId(), fullId1.pageId(), page1);
            }
        } finally {
            mem.stop(true);
        }
    }

    @Test
    public void testLoadedPagesCount() throws Exception {
        PageMemory mem = memory();

        mem.start();

        int expPages = MAX_MEMORY_SIZE / mem.systemPageSize();

        try {
            for (int i = 0; i < expPages * 2; i++) {
                allocatePage(mem);
            }
        } catch (IgniteOutOfMemoryException e) {
            log.error(e.getMessage(), e);

            // Expected.
            assertEquals(mem.loadedPages(), expPages);
        } finally {
            mem.stop(true);
        }
    }

    @Test
    public void testPageTearingSequential() throws Exception {
        PageMemory mem = memory();

        mem.start();

        try {
            int pagesCnt = 1024;

            List<FullPageId> pages = new ArrayList<>(pagesCnt);

            for (int i = 0; i < pagesCnt; i++) {
                FullPageId fullId = allocatePage(mem);

                pages.add(fullId);

                long page = mem.acquirePage(fullId.groupId(), fullId.pageId());

                try {
                    if (i % 64 == 0) {
                        log.info("Writing page [idx=" + i + ", pageId=" + fullId.pageId() + ", page=" + page + ']');
                    }

                    writePage(mem, fullId, page, i + 1);
                } finally {
                    mem.releasePage(fullId.groupId(), fullId.pageId(), page);
                }
            }

            for (int i = 0; i < pagesCnt; i++) {
                FullPageId fullId = pages.get(i);

                long page = mem.acquirePage(fullId.groupId(), fullId.pageId());

                try {
                    if (i % 64 == 0) {
                        log.info("Reading page [idx=" + i + ", pageId=" + fullId.pageId() + ", page=" + page + ']');
                    }

                    readPage(mem, fullId.pageId(), page, i + 1);
                } finally {
                    mem.releasePage(fullId.groupId(), fullId.pageId(), page);
                }
            }
        } finally {
            mem.stop(true);
        }
    }

    @Test
    public void testPageHandleDeallocation() throws Exception {
        PageMemory mem = memory();

        mem.start();

        try {
            int pages = 3 * 1024 * 1024 / (8 * 1024);

            Collection<FullPageId> handles = new HashSet<>();

            for (int i = 0; i < pages; i++) {
                handles.add(allocatePage(mem));
            }

            for (FullPageId fullId : handles) {
                mem.freePage(fullId.groupId(), fullId.pageId());
            }

            for (int i = 0; i < pages; i++) {
                assertFalse(handles.add(allocatePage(mem)));
            }
        } finally {
            mem.stop(true);
        }
    }

    @Test
    public void testPageIdRotation() throws Exception {
        PageMemory mem = memory();

        mem.start();

        try {
            int pages = 5;

            Collection<FullPageId> old = new ArrayList<>();
            Collection<FullPageId> updated = new ArrayList<>();

            for (int i = 0; i < pages; i++) {
                old.add(allocatePage(mem));
            }

            // Check that initial pages are accessible.
            for (FullPageId id : old) {
                long pageApsPtr = mem.acquirePage(id.groupId(), id.pageId());
                try {
                    long pageAddr = mem.writeLock(id.groupId(), id.pageId(), pageApsPtr);

                    assertNotNull(pageAddr);

                    try {
                        PAGE_IO.initNewPage(pageAddr, id.pageId(), mem.realPageSize(id.groupId()));

                        long updId = PageIdUtils.rotatePageId(id.pageId());

                        PageIo.setPageId(pageAddr, updId);

                        updated.add(new FullPageId(updId, id.groupId()));
                    } finally {
                        mem.writeUnlock(id.groupId(), id.pageId(), pageApsPtr, true);
                    }
                } finally {
                    mem.releasePage(id.groupId(), id.pageId(), pageApsPtr);
                }
            }

            // Check that updated pages are inaccessible using old IDs.
            for (FullPageId id : old) {
                long pageApsPtr = mem.acquirePage(id.groupId(), id.pageId());
                try {
                    long pageAddr = mem.writeLock(id.groupId(), id.pageId(), pageApsPtr);

                    if (pageAddr != 0L) {
                        mem.writeUnlock(id.groupId(), id.pageId(), pageApsPtr, false);

                        fail("Was able to acquire page write lock.");
                    }

                    mem.readLock(id.groupId(), id.pageId(), pageApsPtr);

                    if (pageAddr != 0) {
                        mem.readUnlock(id.groupId(), id.pageId(), pageApsPtr);

                        fail("Was able to acquire page read lock.");
                    }
                } finally {
                    mem.releasePage(id.groupId(), id.pageId(), pageApsPtr);
                }
            }

            // Check that updated pages are accessible using new IDs.
            for (FullPageId id : updated) {
                long pageApsPtr = mem.acquirePage(id.groupId(), id.pageId());
                try {
                    long pageAddr = mem.writeLock(id.groupId(), id.pageId(), pageApsPtr);

                    assertNotSame(0L, pageAddr);

                    try {
                        assertEquals(id.pageId(), PageIo.getPageId(pageAddr));
                    } finally {
                        mem.writeUnlock(id.groupId(), id.pageId(), pageApsPtr, false);
                    }

                    pageAddr = mem.readLock(id.groupId(), id.pageId(), pageApsPtr);

                    assertNotSame(0L, pageAddr);

                    try {
                        assertEquals(id.pageId(), PageIo.getPageId(pageAddr));
                    } finally {
                        mem.readUnlock(id.groupId(), id.pageId(), pageApsPtr);
                    }
                } finally {
                    mem.releasePage(id.groupId(), id.pageId(), pageApsPtr);
                }
            }
        } finally {
            mem.stop(true);
        }
    }

    /**
     * Creates new page memory instance.
     */
    protected abstract PageMemory memory();

    /**
     * Fills page with passed value.
     *
     * @param mem Page memory.
     * @param fullId Page ID.
     * @param page Page pointer.
     * @param val Value to write.
     */
    protected void writePage(PageMemory mem, FullPageId fullId, long page, int val) {
        long pageAddr = mem.writeLock(GRP_ID, fullId.pageId(), page);

        try {
            PAGE_IO.initNewPage(pageAddr, fullId.pageId(), mem.realPageSize(fullId.groupId()));

            for (int i = PageIo.COMMON_HEADER_END; i < PAGE_SIZE; i++) {
                PageUtils.putByte(pageAddr, i, (byte) val);
            }
        } finally {
            mem.writeUnlock(GRP_ID, fullId.pageId(), page, true);
        }
    }

    /**
     * Reads a page from page memory and asserts that it is full of expected values.
     *
     * @param mem Page memory.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param expVal Expected value.
     */
    private void readPage(PageMemory mem, long pageId, long page, int expVal) {
        expVal &= 0xFF;

        long pageAddr = mem.readLock(GRP_ID, pageId, page);

        assert pageAddr != 0;

        try {
            for (int i = PageIo.COMMON_HEADER_END; i < PAGE_SIZE; i++) {
                int val = PageUtils.getByte(pageAddr, i) & 0xFF;

                assertEquals(expVal, val, "Unexpected value at position: " + i);
            }
        } finally {
            mem.readUnlock(GRP_ID, pageId, page);
        }
    }

    /**
     * Allocates page.
     *
     * @param mem Memory.
     * @return Page.
     * @throws IgniteInternalCheckedException If failed.
     */
    public static FullPageId allocatePage(PageIdAllocator mem) throws IgniteInternalCheckedException {
        return new FullPageId(mem.allocatePageNoReuse(GRP_ID, PARTITION_ID, PageIdAllocator.FLAG_DATA), GRP_ID);
    }
}
