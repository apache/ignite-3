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
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.writePartitionless;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.Storable;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.pagememory.util.PartitionlessLinks;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Represents row version inside row version chain.
 */
public final class RowVersion implements Storable {
    public static final byte DATA_TYPE = 1;
    private static final int NEXT_LINK_STORE_SIZE_BYTES = PartitionlessLinks.PARTITIONLESS_LINK_SIZE_BYTES;
    private static final int VALUE_SIZE_STORE_SIZE_BYTES = Integer.BYTES;
    private static final int SCHEMA_VERSION_SIZE_BYTES = Short.BYTES;
    public static final int TIMESTAMP_OFFSET = DATA_TYPE_OFFSET + DATA_TYPE_SIZE_BYTES;
    public static final int NEXT_LINK_OFFSET = TIMESTAMP_OFFSET + HYBRID_TIMESTAMP_SIZE;
    public static final int VALUE_SIZE_OFFSET = NEXT_LINK_OFFSET + NEXT_LINK_STORE_SIZE_BYTES;
    public static final int SCHEMA_VERSION_OFFSET = VALUE_SIZE_OFFSET + VALUE_SIZE_STORE_SIZE_BYTES;
    public static final int VALUE_OFFSET = SCHEMA_VERSION_OFFSET + SCHEMA_VERSION_SIZE_BYTES;

    private final int partitionId;

    private long link;

    private final @Nullable HybridTimestamp timestamp;

    private final long nextLink;

    private final int valueSize;

    @IgniteToStringExclude
    private final @Nullable BinaryRow value;

    /**
     * Constructor.
     */
    public RowVersion(int partitionId, long nextLink, @Nullable BinaryRow value) {
        this(partitionId, 0, null, nextLink, value);
    }

    /**
     * Constructor.
     */
    public RowVersion(int partitionId, HybridTimestamp commitTimestamp, long nextLink, @Nullable BinaryRow value) {
        this(partitionId, 0, commitTimestamp, nextLink, value);
    }

    /**
     * Constructor.
     */
    public RowVersion(int partitionId, long link, @Nullable HybridTimestamp timestamp, long nextLink, @Nullable BinaryRow value) {
        this.partitionId = partitionId;
        link(link);

        this.timestamp = timestamp;
        this.nextLink = nextLink;
        this.valueSize = value == null ? 0 : value.tupleSliceLength();
        this.value = value;
    }

    /**
     * Constructor.
     */
    public RowVersion(int partitionId, long link, @Nullable HybridTimestamp timestamp, long nextLink, int valueSize) {
        this.partitionId = partitionId;
        link(link);

        this.timestamp = timestamp;
        this.nextLink = nextLink;
        this.valueSize = valueSize;
        this.value = null;
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
                + SCHEMA_VERSION_SIZE_BYTES;
    }

    @Override
    public void writeRowData(long pageAddr, int dataOff, int payloadSize, boolean newRow) {
        int offset = dataOff;

        PageUtils.putByte(pageAddr, offset, DATA_TYPE);

        offset += Byte.BYTES;

        PageUtils.putShort(pageAddr, offset, (short) payloadSize);
        offset += Short.BYTES;

        offset += HybridTimestamps.writeTimestampToMemory(pageAddr, offset, timestamp());

        offset += writePartitionless(pageAddr + offset, nextLink());

        PageUtils.putInt(pageAddr, offset, valueSize());
        offset += Integer.BYTES;

        if (value != null) {
            PageUtils.putShort(pageAddr, offset, (short) value.schemaVersion());
            offset += Short.BYTES;

            PageUtils.putByteBuffer(pageAddr, offset, value.tupleSlice());
        } else {
            PageUtils.putShort(pageAddr, offset, (short) 0);
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
