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

package org.apache.ignite.internal.storage.pagememory.mv;


import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.writePartitionless;
import static org.apache.ignite.internal.util.GridUnsafe.pageSize;

import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.evict.PageEvictionTracker;
import org.apache.ignite.internal.pagememory.io.DataPageIo;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagememory.util.PageHandler;

class UpdateNextLinkHandler implements PageHandler<Long, Object> {
    private final PageEvictionTracker evictionTracker;

    UpdateNextLinkHandler(PageEvictionTracker evictionTracker) {
        this.evictionTracker = evictionTracker;
    }

    @Override
    public Object run(
            int groupId,
            long pageId,
            long page,
            long pageAddr,
            PageIo io,
            Long nextLink,
            int itemId,
            IoStatisticsHolder statHolder
    ) throws IgniteInternalCheckedException {
        DataPageIo dataIo = (DataPageIo) io;

        int payloadOffset = dataIo.getPayloadOffset(pageAddr, itemId, pageSize(), 0);

        writePartitionless(pageAddr + payloadOffset + RowVersion.NEXT_LINK_OFFSET, nextLink);

        evictionTracker.touchPage(pageId);

        return true;
    }
}

