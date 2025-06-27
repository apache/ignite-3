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
import static org.apache.ignite.internal.util.GridUnsafe.pageSize;

import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.io.DataPageIo;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.util.PageHandler;

class PlainRowVersionOperations implements RowVersionOperations {
    static final PlainRowVersionOperations INSTANCE = new PlainRowVersionOperations();

    private PlainRowVersionOperations() {
        // No-op.
    }

    @Override
    public void removeFromWriteIntentsList(
            AbstractPageMemoryMvPartitionStorage storage,
            Supplier<String> operationInfoSupplier
    ) {
        // No-op as plain row versions are not included in the write intents list.
    }

    @Override
    public long nextWriteIntentLink(long defaultLink) {
        return defaultLink;
    }

    @Override
    public long prevWriteIntentLink() {
        return NULL_LINK;
    }

    @Override
    public PageHandler<HybridTimestamp, Object> converterToCommittedVersion() {
        return UpdateTimestampHandler.INSTANCE;
    }

    private static class UpdateTimestampHandler implements PageHandler<HybridTimestamp, Object> {
        private static final UpdateTimestampHandler INSTANCE = new UpdateTimestampHandler();

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

            HybridTimestamps.writeTimestampToMemory(pageAddr, payloadOffset + RowVersion.TIMESTAMP_OFFSET, timestamp);

            return true;
        }
    }
}
