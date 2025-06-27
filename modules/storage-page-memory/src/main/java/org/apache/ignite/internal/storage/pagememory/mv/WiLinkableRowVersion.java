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
 */
public final class WiLinkableRowVersion extends RowVersion {
    public static final byte DATA_TYPE = 2;

    private static final int WRITE_INTENT_LINKS_SIZE_BYTES = 2 * Long.BYTES + 2 * PARTITIONLESS_LINK_SIZE_BYTES;

    // Write intents list.
    public static final int ROW_ID_MSB_OFFSET = SCHEMA_VERSION_OFFSET + SCHEMA_VERSION_SIZE_BYTES;
    public static final int ROW_ID_LSB_OFFSET = ROW_ID_MSB_OFFSET + Long.BYTES;
    public static final int NEXT_WRITE_INTENT_LINK_OFFSET = ROW_ID_LSB_OFFSET + Long.BYTES;
    public static final int PREV_WRITE_INTENT_LINK_OFFSET = NEXT_WRITE_INTENT_LINK_OFFSET + PARTITIONLESS_LINK_SIZE_BYTES;

    public static final int VALUE_OFFSET = PREV_WRITE_INTENT_LINK_OFFSET + PARTITIONLESS_LINK_SIZE_BYTES;

    private final RowId rowId;

    private final long nextWriteIntentLink;
    private final long prevWriteIntentLink;

    /**
     * Constructor.
     */
    public WiLinkableRowVersion(
            RowId rowId,
            int partitionId,
            long nextLink,
            long nextWriteIntentLink,
            long prevWriteIntentLink,
            @Nullable BinaryRow value
    ) {
        this(
                rowId,
                partitionId,
                NULL_LINK,
                null,
                nextLink,
                nextWriteIntentLink,
                prevWriteIntentLink,
                value == null ? 0 : value.tupleSliceLength(),
                value
        );
    }

    /**
     * Constructor.
     */
    public WiLinkableRowVersion(
            RowId rowId,
            int partitionId,
            long link,
            @Nullable HybridTimestamp commitTimestamp,
            long nextLink,
            long nextWriteIntentLink,
            long prevWriteIntentLink,
            int valueSize
    ) {
        this(rowId, partitionId, link, commitTimestamp, nextLink, nextWriteIntentLink, prevWriteIntentLink, valueSize, null);
    }

    /**
     * Constructor.
     */
    public WiLinkableRowVersion(
            RowId rowId,
            int partitionId,
            long link,
            @Nullable HybridTimestamp timestamp,
            long nextLink,
            long nextWriteIntentLink,
            long prevWriteIntentLink,
            int valueSize,
            @Nullable BinaryRow value
    ) {
        super(partitionId, link, timestamp, nextLink, valueSize, value);

        this.rowId = rowId;
        this.nextWriteIntentLink = nextWriteIntentLink;
        this.prevWriteIntentLink = prevWriteIntentLink;
    }

    public RowId rowId() {
        return rowId;
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
        return DATA_TYPE;
    }

    @Override
    protected int valueOffset() {
        return VALUE_OFFSET;
    }

    @Override
    protected void writeHeader(long pageAddr, int dataOff) {
        super.writeHeader(pageAddr, dataOff);

        putLong(pageAddr, dataOff + ROW_ID_MSB_OFFSET, rowId.mostSignificantBits());
        putLong(pageAddr, dataOff + ROW_ID_LSB_OFFSET, rowId.leastSignificantBits());
        writePartitionless(pageAddr + dataOff + NEXT_WRITE_INTENT_LINK_OFFSET, nextWriteIntentLink);
        writePartitionless(pageAddr + dataOff + PREV_WRITE_INTENT_LINK_OFFSET, prevWriteIntentLink);
    }

    @Override
    protected void writeHeader(ByteBuffer pageBuf) {
        super.writeHeader(pageBuf);

        pageBuf.putLong(rowId.mostSignificantBits());
        pageBuf.putLong(rowId.leastSignificantBits());
        PartitionlessLinks.writeToBuffer(pageBuf, nextWriteIntentLink);
        PartitionlessLinks.writeToBuffer(pageBuf, prevWriteIntentLink);
    }

    @Override
    RowVersionOperations operations() {
        return new WiLinkableRowVersionOperations(this);
    }

    @Override
    public String toString() {
        return S.toString(WiLinkableRowVersion.class, this);
    }
}
