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

import static org.apache.ignite.internal.pagememory.util.PageIdUtils.NULL_LINK;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putByte;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.writePartitionless;
import static org.apache.ignite.internal.storage.pagememory.mv.WriteIntentListSupport.removeNodeFromWriteIntentsList;
import static org.apache.ignite.internal.util.GridUnsafe.pageSize;

import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.Storable;
import org.apache.ignite.internal.pagememory.io.DataPageIo;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.util.PageHandler;

class WiLinkableRowVersionOperations implements RowVersionOperations {
    private final WiLinkableRowVersion rowVersion;

    WiLinkableRowVersionOperations(WiLinkableRowVersion rowVersion) {
        this.rowVersion = rowVersion;
    }

    @Override
    public void removeFromWriteIntentsList(
            AbstractPageMemoryMvPartitionStorage storage,
            Supplier<String> operationInfoSupplier
    ) {
        assert storage instanceof PersistentPageMemoryMvPartitionStorage;

        // We don't use rowVersion as WI links stored inside it might be stale. Instead, we pass its link
        // so that removeNodeFromWriteIntentsList() can read the latest WI links from the page.
        removeNodeFromWriteIntentsList(rowVersion.link(), (PersistentPageMemoryMvPartitionStorage) storage, operationInfoSupplier);
    }

    @Override
    public long nextWriteIntentLink(long fallbackLink) {
        return rowVersion.nextWriteIntentLink();
    }

    @Override
    public long prevWriteIntentLink() {
        return rowVersion.prevWriteIntentLink();
    }

    @Override
    public PageHandler<HybridTimestamp, Object> converterToCommittedVersion() {
        return ConvertToCommittedVersion.INSTANCE;
    }

    private static class ConvertToCommittedVersion implements PageHandler<HybridTimestamp, Object> {
        private static final ConvertToCommittedVersion INSTANCE = new ConvertToCommittedVersion();

        @Override
        public Object run(
                int groupId,
                long pageId,
                long page,
                long pageAddr,
                PageIo io,
                HybridTimestamp timestamp,
                int itemId
        ) {
            DataPageIo dataIo = (DataPageIo) io;

            int payloadOffset = dataIo.getPayloadOffset(pageAddr, itemId, pageSize(), 0);

            // Change data type to committed as we represent write intents and committed versions with different types.
            putByte(pageAddr, payloadOffset + Storable.DATA_TYPE_OFFSET, WiLinkableRowVersion.COMMITTED_DATA_TYPE);

            HybridTimestamps.writeTimestampToMemory(pageAddr, payloadOffset + RowVersion.TIMESTAMP_OFFSET, timestamp);
            writePartitionless(pageAddr + payloadOffset + WiLinkableRowVersion.NEXT_WRITE_INTENT_LINK_OFFSET, NULL_LINK);
            writePartitionless(pageAddr + payloadOffset + WiLinkableRowVersion.PREV_WRITE_INTENT_LINK_OFFSET, NULL_LINK);

            return true;
        }
    }
}
