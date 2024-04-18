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

package org.apache.ignite.internal.pagememory.persistence.replacement;

import static org.apache.ignite.internal.pagememory.persistence.PageHeader.fullPageId;
import static org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory.INVALID_REL_PTR;
import static org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory.PAGE_OVERHEAD;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.freelist.io.PagesListMetaIo;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.persistence.LoadedPagesMap;
import org.apache.ignite.internal.pagememory.persistence.PageHeader;
import org.apache.ignite.internal.pagememory.persistence.PagePool;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory.Segment;
import org.apache.ignite.internal.pagememory.persistence.ReplaceCandidate;

/**
 * Random-LRU page replacement policy implementation.
 */
public class RandomLruPageReplacementPolicy extends PageReplacementPolicy {
    /** Number of random pages that will be picked for eviction. */
    public static final int RANDOM_PAGES_EVICT_NUM = 5;

    private static final double FULL_SCAN_THRESHOLD = 0.4;

    /**
     * Constructor.
     *
     * @param seg Page memory segment.
     */
    protected RandomLruPageReplacementPolicy(Segment seg) {
        super(seg);
    }

    /** {@inheritDoc} */
    @Override
    public long replace() throws IgniteInternalCheckedException {
        final ThreadLocalRandom rnd = ThreadLocalRandom.current();

        LoadedPagesMap loadedPages = seg.loadedPages();
        PagePool pool = seg.pool();

        final int cap = loadedPages.capacity();

        // With big number of random picked pages we may fall into infinite loop, because
        // every time the same page may be found.
        Set<Long> ignored = null;

        long relRmvAddr = INVALID_REL_PTR;

        int iterations = 0;

        while (true) {
            long cleanAddr = INVALID_REL_PTR;
            long cleanTs = Long.MAX_VALUE;
            long dirtyAddr = INVALID_REL_PTR;
            long dirtyTs = Long.MAX_VALUE;
            long metaAddr = INVALID_REL_PTR;
            long metaTs = Long.MAX_VALUE;

            for (int i = 0; i < RANDOM_PAGES_EVICT_NUM; i++) {
                ++iterations;

                if (iterations > pool.pages() * FULL_SCAN_THRESHOLD) {
                    break;
                }

                // We need to lookup for pages only in current segment for thread safety,
                // so peeking random memory will lead to checking for found page segment.
                // It's much faster to check available pages for segment right away.
                ReplaceCandidate nearest = loadedPages.getNearestAt(rnd.nextInt(cap));

                assert nearest != null && nearest.relativePointer() != INVALID_REL_PTR;

                long rndAddr = nearest.relativePointer();

                int partGen = nearest.generation();

                final long absPageAddr = seg.absolute(rndAddr);

                FullPageId fullId = fullPageId(absPageAddr);

                // Check page mapping consistency.
                assert fullId.equals(nearest.fullId()) : "Invalid page mapping [tableId=" + nearest.fullId()
                        + ", actual=" + fullId + ", nearest=" + nearest;

                boolean outdated = partGen < seg.partGeneration(fullId.groupId(), partitionId(fullId.pageId()));

                if (outdated) {
                    return seg.refreshOutdatedPage(fullId.groupId(), fullId.pageId(), true);
                }

                boolean pinned = PageHeader.isAcquired(absPageAddr);

                boolean skip = ignored != null && ignored.contains(rndAddr);

                final boolean dirty = PageHeader.dirty(absPageAddr);

                if (relRmvAddr == rndAddr || pinned || skip || dirty) {
                    i--;

                    continue;
                }

                final long pageTs = PageHeader.readTimestamp(absPageAddr);

                final boolean storMeta = isStoreMetadataPage(absPageAddr);

                if (pageTs < cleanTs && !dirty && !storMeta) {
                    cleanAddr = rndAddr;

                    cleanTs = pageTs;
                } else if (pageTs < dirtyTs && dirty && !storMeta) {
                    dirtyAddr = rndAddr;

                    dirtyTs = pageTs;
                } else if (pageTs < metaTs && storMeta) {
                    metaAddr = rndAddr;

                    metaTs = pageTs;
                }

                if (cleanAddr != INVALID_REL_PTR) {
                    relRmvAddr = cleanAddr;
                } else if (dirtyAddr != INVALID_REL_PTR) {
                    relRmvAddr = dirtyAddr;
                } else {
                    relRmvAddr = metaAddr;
                }
            }

            if (relRmvAddr == INVALID_REL_PTR) {
                return tryToFindSequentially(cap);
            }

            final long absRmvAddr = seg.absolute(relRmvAddr);

            final FullPageId fullPageId = fullPageId(absRmvAddr);

            if (!seg.tryToRemovePage(fullPageId, absRmvAddr)) {
                if (iterations > 10) {
                    if (ignored == null) {
                        ignored = new HashSet<>();
                    }

                    ignored.add(relRmvAddr);
                }

                if (iterations > seg.pool().pages() * FULL_SCAN_THRESHOLD) {
                    return tryToFindSequentially(cap);
                }

                continue;
            }

            return relRmvAddr;
        }
    }

    /**
     * Return {@code true} if page is related to metadata.
     *
     * @param absPageAddr Absolute page address
     */
    private boolean isStoreMetadataPage(long absPageAddr) {
        try {
            long dataAddr = absPageAddr + PAGE_OVERHEAD;

            PageIo io = seg.ioRegistry().resolve(dataAddr);

            return io instanceof PagesListMetaIo;
        } catch (IgniteInternalCheckedException ignored) {
            return false;
        }
    }

    /**
     * Will scan all segment pages to find one to evict it.
     *
     * @param cap Capacity.
     */
    private long tryToFindSequentially(int cap) throws IgniteInternalCheckedException {
        assert seg.getWriteHoldCount() > 0;

        long prevAddr = INVALID_REL_PTR;

        LoadedPagesMap loadedPages = seg.loadedPages();

        for (int i = 0; i < cap; i++) {
            final ReplaceCandidate nearest = loadedPages.getNearestAt(i);

            assert nearest != null && nearest.relativePointer() != INVALID_REL_PTR;

            final long addr = nearest.relativePointer();

            int partGen = nearest.generation();

            final long absPageAddr = seg.absolute(addr);

            FullPageId fullId = fullPageId(absPageAddr);

            if (partGen < seg.partGeneration(fullId.groupId(), partitionId(fullId.pageId()))) {
                return seg.refreshOutdatedPage(fullId.groupId(), fullId.pageId(), true);
            }

            boolean pinned = PageHeader.isAcquired(absPageAddr);

            if (addr == prevAddr || pinned) {
                continue;
            }

            final long absEvictAddr = seg.absolute(addr);

            final FullPageId fullPageId = fullPageId(absEvictAddr);

            if (seg.tryToRemovePage(fullPageId, absEvictAddr)) {
                return addr;
            }

            prevAddr = addr;
        }

        throw seg.oomException("no pages to replace");
    }
}
