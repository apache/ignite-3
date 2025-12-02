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

import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.readPartitionless;

import org.apache.ignite.internal.pagememory.Storable;
import org.apache.ignite.internal.pagememory.datapage.PageMemoryTraversal;
import org.apache.ignite.internal.pagememory.io.DataPagePayload;
import org.apache.ignite.internal.pagememory.util.PageUtils;

/**
 * Traversal for reading a row version's write intent links by its link.
 */
class ReadWriteIntentLinks implements PageMemoryTraversal<Void> {
    private final int partitionId;

    private WriteIntentLinks result;

    private long nextWiLink;
    private long prevWiLink;

    ReadWriteIntentLinks(int partitionId) {
        this.partitionId = partitionId;
    }

    @Override
    public long consumePagePayload(long link, long pageAddr, DataPagePayload payload, Void unused) {
        return readFullOrInitiateReadFragmented(pageAddr, payload);
    }

    private long readFullOrInitiateReadFragmented(long pageAddr, DataPagePayload payload) {
        byte dataType = PageUtils.getByte(pageAddr, payload.offset() + Storable.DATA_TYPE_OFFSET);

        assert dataType == WiLinkableRowVersion.DATA_TYPE;

        nextWiLink = readPartitionless(partitionId, pageAddr, payload.offset() + WiLinkableRowVersion.NEXT_WRITE_INTENT_LINK_OFFSET);
        prevWiLink = readPartitionless(partitionId, pageAddr, payload.offset() + WiLinkableRowVersion.PREV_WRITE_INTENT_LINK_OFFSET);

        return STOP_TRAVERSAL;
    }

    @Override
    public void finish() {
        if (result != null) {
            return;
        }

        result = new WriteIntentLinks(nextWiLink, prevWiLink);
    }

    WriteIntentLinks result() {
        return result;
    }
}
