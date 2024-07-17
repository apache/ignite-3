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

import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionIdFromLink;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.readPartitionless;

import java.nio.ByteBuffer;
import java.util.function.Predicate;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.datapage.PageMemoryTraversal;
import org.apache.ignite.internal.pagememory.io.DataPagePayload;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowImpl;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Traversal for reading a row version by its link. Loads the version value conditionally.
 */
class ReadRowVersion implements PageMemoryTraversal<Predicate<HybridTimestamp>> {
    private final int partitionId;

    private RowVersion result;

    private boolean readingFirstSlot = true;

    private long firstFragmentLink;

    private @Nullable HybridTimestamp timestamp;

    private long nextLink;

    private int schemaVersion;

    private final ReadRowVersionValue readRowVersionValue = new ReadRowVersionValue();

    ReadRowVersion(int partitionId) {
        this.partitionId = partitionId;
    }

    @Override
    public long consumePagePayload(long link, long pageAddr, DataPagePayload payload, Predicate<HybridTimestamp> loadValue) {
        if (readingFirstSlot) {
            readingFirstSlot = false;

            return readFullOrInitiateReadFragmented(link, pageAddr, payload, loadValue);
        } else {
            return readRowVersionValue.consumePagePayload(link, pageAddr, payload, null);
        }
    }

    private long readFullOrInitiateReadFragmented(long link, long pageAddr, DataPagePayload payload, Predicate<HybridTimestamp> loadValue) {
        firstFragmentLink = link;

        timestamp = HybridTimestamps.readTimestamp(pageAddr, payload.offset() + RowVersion.TIMESTAMP_OFFSET);
        nextLink = readPartitionless(partitionId, pageAddr, payload.offset() + RowVersion.NEXT_LINK_OFFSET);
        schemaVersion = Short.toUnsignedInt(PageUtils.getShort(pageAddr, payload.offset() + RowVersion.SCHEMA_VERSION_OFFSET));

        if (!loadValue.test(timestamp)) {
            int valueSize = PageUtils.getInt(pageAddr, payload.offset() + RowVersion.VALUE_SIZE_OFFSET);

            result = new RowVersion(partitionIdFromLink(link), firstFragmentLink, timestamp, nextLink, valueSize);

            return STOP_TRAVERSAL;
        }

        return readRowVersionValue.consumePagePayload(link, pageAddr, payload, null);
    }

    @Override
    public void finish() {
        if (result != null) {
            // we did not read the value itself, so we don't need to invoke readRowVersionValue.finish()
            return;
        }

        readRowVersionValue.finish();

        byte[] valueBytes = readRowVersionValue.result();

        BinaryRow row = valueBytes.length == 0
                ? null
                : new BinaryRowImpl(schemaVersion, ByteBuffer.wrap(valueBytes).order(BinaryTuple.ORDER));

        result = new RowVersion(partitionIdFromLink(firstFragmentLink), firstFragmentLink, timestamp, nextLink, row);
    }

    RowVersion result() {
        return result;
    }

    void reset() {
        result = null;
        readingFirstSlot = true;
        readRowVersionValue.reset();
    }
}
