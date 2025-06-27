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

import static org.apache.ignite.internal.hlc.HybridTimestamp.HYBRID_TIMESTAMP_SIZE;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.NULL_LINK;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.writePartitionless;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.Storable;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.pagememory.util.PartitionlessLinks;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Represents row version inside row version chain.
 */
public final class RowVersion implements Storable {
    public static final byte DATA_TYPE = 0;
    private static final int NEXT_LINK_STORE_SIZE_BYTES = PartitionlessLinks.PARTITIONLESS_LINK_SIZE_BYTES;
    private static final int VALUE_SIZE_STORE_SIZE_BYTES = Integer.BYTES;
    private static final int SCHEMA_VERSION_SIZE_BYTES = Short.BYTES;
    private static final int WI_LINKS_SIZE = 4 * Long.BYTES;

    public static final int TIMESTAMP_OFFSET = DATA_TYPE_OFFSET + DATA_TYPE_SIZE_BYTES;
    public static final int NEXT_LINK_OFFSET = TIMESTAMP_OFFSET + HYBRID_TIMESTAMP_SIZE;

    // Write intent list.
    public static final int ROW_ID_LSB_OFFSET = NEXT_LINK_OFFSET + NEXT_LINK_STORE_SIZE_BYTES;
    public static final int ROW_ID_MSB_OFFSET = ROW_ID_LSB_OFFSET + Long.BYTES;
    public static final int PREV_WI_LINK_OFFSET = ROW_ID_MSB_OFFSET + Long.BYTES;
    public static final int NEXT_WI_LINK_OFFSET = PREV_WI_LINK_OFFSET + Long.BYTES;

    public static final int VALUE_SIZE_OFFSET = NEXT_WI_LINK_OFFSET + Long.BYTES;
    public static final int SCHEMA_VERSION_OFFSET = VALUE_SIZE_OFFSET + VALUE_SIZE_STORE_SIZE_BYTES;
    public static final int VALUE_OFFSET = SCHEMA_VERSION_OFFSET + SCHEMA_VERSION_SIZE_BYTES;

    private final int partitionId;

    private long link;

    private final @Nullable HybridTimestamp timestamp;

    private final long nextLink;

    private final int valueSize;

    private final RowId rowId;

    /* Previous Write intent page link */
    private final long prev;

    /* Next Write intent page link */
    private final long next;

    @IgniteToStringExclude
    private final @Nullable BinaryRow value;

    /**
     * Constructor.
     */
    public RowVersion(RowId rowId, int partitionId, long nextLink, @Nullable BinaryRow value, long prev, long next) {
        this(rowId, partitionId, 0, null, nextLink, value, prev, next, value == null ? 0 : value.tupleSliceLength());
    }

    /**
     * Constructor.
     */
    public RowVersion(RowId rowId, int partitionId, HybridTimestamp commitTimestamp, long nextLink, @Nullable BinaryRow value,
            long prev, long next) {
        this(rowId, partitionId, 0, commitTimestamp, nextLink, value, prev, next, value == null ? 0 : value.tupleSliceLength());
    }

    /**
     * Constructor.
     */
    public RowVersion(RowId rowId, int partitionId, long link, @Nullable HybridTimestamp commitTimestamp, long nextLink,
            long prev, long next, int valueSize) {
        this(rowId, partitionId, link, commitTimestamp, nextLink, null, prev, next, valueSize);
    }

    /**
     * Constructor.
     */
    public RowVersion(
            RowId rowId,
            int partitionId,
            long link,
            @Nullable HybridTimestamp timestamp,
            long nextLink,
            @Nullable BinaryRow value,
            long prev,
            long next,
            int valueSize
    ) {
        this.partitionId = partitionId;
        link(link);

        this.timestamp = timestamp;
        this.nextLink = nextLink;
        this.valueSize = valueSize;
        this.value = value;
        this.rowId = rowId;
        this.prev = prev;
        this.next = next;
    }

    public @Nullable HybridTimestamp timestamp() {
        return timestamp;
    }

    /**
     * Returns partitionless link of the next version or {@code 0} if this version is the last in the chain (i.e. it's the oldest version).
     */
    public long nextLink() {
        return nextLink;
    }

    public int valueSize() {
        return valueSize;
    }

    public @Nullable BinaryRow value() {
        return value;
    }

    public boolean hasNextLink() {
        return nextLink != NULL_LINK;
    }

    boolean isTombstone() {
        return valueSize == 0;
    }

    boolean isUncommitted() {
        return timestamp == null;
    }

    boolean isCommitted() {
        return timestamp != null;
    }

    public RowId rowId() {
        return rowId;
    }

    public long getPrev() {
        return prev;
    }

    public long getNext() {
        return next;
    }

    @Override
    public void link(long link) {
        this.link = link;
    }

    @Override
    public long link() {
        return link;
    }

    @Override
    public int partition() {
        return partitionId;
    }

    @Override
    public int size() {
        return headerSize() + valueSize;
    }

    @Override
    public int headerSize() {
        return HYBRID_TIMESTAMP_SIZE + DATA_TYPE_SIZE_BYTES + NEXT_LINK_STORE_SIZE_BYTES + VALUE_SIZE_STORE_SIZE_BYTES
                + SCHEMA_VERSION_SIZE_BYTES + WI_LINKS_SIZE;
    }

    @Override
    public void writeRowData(long pageAddr, int dataOff, int payloadSize, boolean newRow) {
        PageUtils.putShort(pageAddr, dataOff, (short) payloadSize);
        dataOff += Short.BYTES;

        PageUtils.putByte(pageAddr, dataOff + DATA_TYPE_OFFSET, DATA_TYPE);

        HybridTimestamps.writeTimestampToMemory(pageAddr, dataOff + TIMESTAMP_OFFSET, timestamp());

        writePartitionless(pageAddr + dataOff + NEXT_LINK_OFFSET, nextLink());

        putLong(pageAddr, dataOff + ROW_ID_LSB_OFFSET, rowId.leastSignificantBits());
        putLong(pageAddr, dataOff + ROW_ID_MSB_OFFSET, rowId.mostSignificantBits());
        putLong(pageAddr, dataOff + PREV_WI_LINK_OFFSET, prev);
        putLong(pageAddr, dataOff + NEXT_WI_LINK_OFFSET, next);

        PageUtils.putInt(pageAddr, dataOff + VALUE_SIZE_OFFSET, valueSize());

        if (value != null) {
            PageUtils.putShort(pageAddr, dataOff + SCHEMA_VERSION_OFFSET, (short) value.schemaVersion());

            PageUtils.putByteBuffer(pageAddr, dataOff + VALUE_OFFSET, value.tupleSlice());
        } else {
            PageUtils.putShort(pageAddr, dataOff + SCHEMA_VERSION_OFFSET, (short) 0);
        }
    }

    @Override
    public void writeFragmentData(ByteBuffer pageBuf, int rowOff, int payloadSize) {
        int headerSize = headerSize();

        int bufferOffset;
        int bufferSize;

        if (rowOff == 0) {
            // first fragment
            assert headerSize <= payloadSize : "Header must entirely fit in the first fragment, but header size is "
                    + headerSize + " and payload size is " + payloadSize;

            pageBuf.put(DATA_TYPE);

            HybridTimestamps.writeTimestampToBuffer(pageBuf, timestamp());

            PartitionlessLinks.writeToBuffer(pageBuf, nextLink());

            pageBuf.putLong(rowId.leastSignificantBits());
            pageBuf.putLong(rowId.mostSignificantBits());
            pageBuf.putLong(prev);
            pageBuf.putLong(next);

            pageBuf.putInt(valueSize());

            pageBuf.putShort(value == null ? 0 : (short) value.schemaVersion());

            bufferOffset = 0;
            bufferSize = payloadSize - headerSize;
        } else {
            // non-first fragment
            assert rowOff >= headerSize;

            bufferOffset = rowOff - headerSize;
            bufferSize = payloadSize;
        }

        if (value != null) {
            Storable.putValueBufferIntoPage(pageBuf, value.tupleSlice(), bufferOffset, bufferSize);
        }
    }

    @Override
    public String toString() {
        return S.toString(RowVersion.class, this);
    }
}
