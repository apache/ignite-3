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

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.NULL_LINK;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.PARTITIONLESS_LINK_SIZE_BYTES;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.writePartitionless;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.util.PartitionlessLinks;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * {@link RowVersion} extension which allows the represented write intent to be included in the write intents list.
 *
 * <p>The write intents list is a doubly linked list organized using next and previous WI links stored in each write intent.
 * The list head is stored in the partition metadata.
 *
 * <p>Each write intent also stores the {@link RowId} of the row it belongs to, so that pending rows can be efficiently found
 * on partition recovery. The higher half of the {@link RowId} (most significant bits) is stored in a dedicated field, while the lower half
 * (least significant bits) is stored in the timestamp field, as row IDs only make sense for write intents,
 * and timestamps only for committed versions.
 *
 * <p>The overall structure is as follows:
 * <pre>
 * ...
 *           ^                     |
 *           |                     v
 * Chain 1 = [rowId, timestamp, row] -> ... -> []
 *           ^                     |
 *           |                     v
 * Chain 2 = [rowId, timestamp, row] -> ... -> []
 *           ^                     |
 *           |                     v
 * ...</pre>
 */
public final class WiLinkableRowVersion extends RowVersion {
    public static final byte WRITE_INTENT_DATA_TYPE = 2;
    public static final byte COMMITTED_DATA_TYPE = 3;

    /** Half of a UUID (8 bytes) and two partitionless links (6 bytes each). */
    private static final int WRITE_INTENT_LINKS_SIZE_BYTES = Long.BYTES + 2 * PARTITIONLESS_LINK_SIZE_BYTES;

    // Write intents list.

    // Row ID least significant bits are stored at the timestamp slot as row ID and timestamp are mutually exclusive.
    public static final int ROW_ID_LSB_OFFSET = TIMESTAMP_OFFSET;

    public static final int ROW_ID_MSB_OFFSET = SCHEMA_VERSION_OFFSET + SCHEMA_VERSION_SIZE_BYTES;
    public static final int PREV_WRITE_INTENT_LINK_OFFSET = ROW_ID_MSB_OFFSET + Long.BYTES;
    public static final int NEXT_WRITE_INTENT_LINK_OFFSET = PREV_WRITE_INTENT_LINK_OFFSET + PARTITIONLESS_LINK_SIZE_BYTES;

    public static final int VALUE_OFFSET = NEXT_WRITE_INTENT_LINK_OFFSET + PARTITIONLESS_LINK_SIZE_BYTES;

    private final @Nullable RowId rowId;

    private final long prevWriteIntentLink;
    private final long nextWriteIntentLink;

    /**
     * Constructor.
     */
    public WiLinkableRowVersion(
            RowId rowId,
            int partitionId,
            long nextLink,
            long prevWriteIntentLink,
            long nextWriteIntentLink,
            @Nullable BinaryRow value
    ) {
        this(
                rowId,
                partitionId,
                NULL_LINK,
                null,
                nextLink,
                prevWriteIntentLink,
                nextWriteIntentLink,
                value == null ? 0 : value.tupleSliceLength(),
                value
        );
    }

    /**
     * Constructor.
     */
    public WiLinkableRowVersion(
            @Nullable RowId rowId,
            int partitionId,
            long link,
            @Nullable HybridTimestamp timestamp,
            long nextLink,
            long prevWriteIntentLink,
            long nextWriteIntentLink,
            int valueSize,
            @Nullable BinaryRow value
    ) {
        super(partitionId, link, timestamp, nextLink, valueSize, value);

        assert (timestamp == null) ^ (rowId == null) : "Either timestamp or rowId must be null";

        this.rowId = rowId;
        this.prevWriteIntentLink = prevWriteIntentLink;
        this.nextWriteIntentLink = nextWriteIntentLink;
    }

    public RowId requiredRowId() {
        return requireNonNull(rowId);
    }

    public long nextWriteIntentLink() {
        return nextWriteIntentLink;
    }

    public long prevWriteIntentLink() {
        return prevWriteIntentLink;
    }

    @Override
    public int headerSize() {
        return super.headerSize() + WRITE_INTENT_LINKS_SIZE_BYTES;
    }

    @Override
    protected byte dataType() {
        return isWriteIntent() ? WRITE_INTENT_DATA_TYPE : COMMITTED_DATA_TYPE;
    }

    private boolean isWriteIntent() {
        return timestamp() == null;
    }

    @Override
    protected int valueOffset() {
        return VALUE_OFFSET;
    }

    @Override
    protected void writeHeader(long pageAddr, int dataOff) {
        super.writeHeader(pageAddr, dataOff);

        if (isWriteIntent()) {
            assert rowId != null;

            putLong(pageAddr, dataOff + ROW_ID_MSB_OFFSET, rowId.mostSignificantBits());
            putLong(pageAddr, dataOff + ROW_ID_LSB_OFFSET, rowId.leastSignificantBits());
        } else {
            assert rowId == null;

            putLong(pageAddr, dataOff + ROW_ID_MSB_OFFSET, 0);
            // We don't write row ID LSB in committed versions as it overlaps with timestamp.
        }

        writePartitionless(pageAddr + dataOff + PREV_WRITE_INTENT_LINK_OFFSET, prevWriteIntentLink);
        writePartitionless(pageAddr + dataOff + NEXT_WRITE_INTENT_LINK_OFFSET, nextWriteIntentLink);
    }

    @Override
    protected void writeHeader(ByteBuffer pageBuf) {
        super.writeHeader(pageBuf);

        if (isWriteIntent()) {
            assert rowId != null;

            pageBuf.putLong(rowId.mostSignificantBits());
            // Row ID LSB is already written in the timestamp slot by writeToTimestampSlot().
        } else {
            assert rowId == null;

            // Writing row ID MSB as 0 for committed versions.
            pageBuf.putLong(0L);
        }

        PartitionlessLinks.writeToBuffer(pageBuf, prevWriteIntentLink);
        PartitionlessLinks.writeToBuffer(pageBuf, nextWriteIntentLink);
    }

    @Override
    protected void writeTimestamp(long pageAddr, int dataOff) {
        // Timestamp occupies the same slot as rowId LSB in write intents, so we only write it for committed versions.
        if (!isWriteIntent()) {
            super.writeTimestamp(pageAddr, dataOff);
        }
    }

    @Override
    protected void writeToTimestampSlot(ByteBuffer pageBuf) {
        // Timestamp occupies the same slot as rowId LSB in write intents.
        if (isWriteIntent()) {
            assert rowId != null;

            pageBuf.putLong(rowId.leastSignificantBits());
        } else {
            super.writeToTimestampSlot(pageBuf);
        }
    }

    @Override
    RowVersionOperations operations() {
        return new WiLinkableRowVersionOperations(this);
    }

    @Override
    public String toString() {
        return S.toString(WiLinkableRowVersion.class, this, super.toString());
    }
}
