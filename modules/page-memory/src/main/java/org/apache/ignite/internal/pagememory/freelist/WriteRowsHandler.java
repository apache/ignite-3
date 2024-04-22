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

package org.apache.ignite.internal.pagememory.freelist;

import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.Storable;
import org.apache.ignite.internal.pagememory.io.DataPageIo;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagememory.util.PageHandler;
import org.apache.ignite.internal.util.CachedIterator;

final class WriteRowsHandler implements PageHandler<CachedIterator<Storable>, Integer> {
    private final FreeListImpl freeList;

    public WriteRowsHandler(FreeListImpl freeList) {
        this.freeList = freeList;
    }

    /** {@inheritDoc} */
    @Override
    public Integer run(
            int cacheId,
            long pageId,
            long page,
            long pageAddr,
            PageIo iox,
            CachedIterator<Storable> it,
            int written,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        DataPageIo io = (DataPageIo) iox;

        // Fill the page up to the end.
        while (written != FreeListImpl.COMPLETE || (!freeList.evictionTracker.evictionRequired() && it.hasNext())) {
            Storable row = it.get();

            if (written == FreeListImpl.COMPLETE) {
                row = it.next();

                // If the data row was completely written without remainder, proceed to the next.
                if ((written = freeList.writeWholePages(row, statHolder)) == FreeListImpl.COMPLETE) {
                    continue;
                }

                if (io.getFreeSpace(pageAddr) < row.size() - written) {
                    break;
                }
            }

            written = freeList.writeRowHandler().addRow(pageId, pageAddr, io, row, written);

            assert written == FreeListImpl.COMPLETE;

            freeList.evictionTracker.touchPage(pageId);
        }

        freeList.writeRowHandler().putPage(io.getFreeSpace(pageAddr), pageId, pageAddr, statHolder);

        return written;
    }
}
