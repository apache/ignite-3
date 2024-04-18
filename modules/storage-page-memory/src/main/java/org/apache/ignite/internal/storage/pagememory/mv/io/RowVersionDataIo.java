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

package org.apache.ignite.internal.storage.pagememory.mv.io;

import static org.apache.ignite.internal.pagememory.util.PageUtils.putByteBuffer;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putShort;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.writePartitionless;
import static org.apache.ignite.internal.storage.pagememory.mv.MvPageTypes.T_ROW_VERSION_DATA_IO;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.pagememory.io.AbstractDataPageIo;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.util.PartitionlessLinks;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.pagememory.mv.HybridTimestamps;
import org.apache.ignite.internal.storage.pagememory.mv.RowVersion;
import org.jetbrains.annotations.Nullable;

/**
 * Data pages IO for {@link RowVersion}.
 */
public class RowVersionDataIo extends AbstractDataPageIo<RowVersion> {
    /** I/O versions. */
    public static final IoVersions<RowVersionDataIo> VERSIONS = new IoVersions<>(new RowVersionDataIo(1));

    /**
     * Constructor.
     *
     * @param ver Page format version.
     */
    protected RowVersionDataIo(int ver) {
        super(T_ROW_VERSION_DATA_IO, ver);
    }

    @Override
    protected void writeRowData(long pageAddr, int dataOff, int payloadSize, RowVersion rowVersion, boolean newRow) {
        assertPageType(pageAddr);

        int offset = dataOff;

        putShort(pageAddr, offset, (short) payloadSize);
        offset += Short.BYTES;

        offset += HybridTimestamps.writeTimestampToMemory(pageAddr, offset, rowVersion.timestamp());

        offset += writePartitionless(pageAddr + offset, rowVersion.nextLink());

        putInt(pageAddr, offset, rowVersion.valueSize());
        offset += Integer.BYTES;

        BinaryRow row = rowVersion.value();

        if (row != null) {
            putShort(pageAddr, offset, (short) row.schemaVersion());
            offset += Short.BYTES;

            putByteBuffer(pageAddr, offset, row.tupleSlice());
        } else {
            putShort(pageAddr, offset, (short) 0);
        }
    }

    @Override
    protected void writeFragmentData(RowVersion rowVersion, ByteBuffer pageBuf, int rowOff, int payloadSize) {
        assertPageType(pageBuf);

        int headerSize = rowVersion.headerSize();

        BinaryRow row = rowVersion.value();

        int bufferOffset;
        int bufferSize;

        if (rowOff == 0) {
            // first fragment
            assert headerSize <= payloadSize : "Header must entirely fit in the first fragment, but header size is "
                    + headerSize + " and payload size is " + payloadSize;

            HybridTimestamps.writeTimestampToBuffer(pageBuf, rowVersion.timestamp());

            PartitionlessLinks.writeToBuffer(pageBuf, rowVersion.nextLink());

            pageBuf.putInt(rowVersion.valueSize());

            pageBuf.putShort(row == null ? 0 : (short) row.schemaVersion());

            bufferOffset = 0;
            bufferSize = payloadSize - headerSize;
        } else {
            // non-first fragment
            assert rowOff >= headerSize;

            bufferOffset = rowOff - headerSize;
            bufferSize = payloadSize;
        }

        if (row != null) {
            putValueBufferIntoPage(pageBuf, row.tupleSlice(), bufferOffset, bufferSize);
        }
    }

    /**
     * Updates timestamp leaving the rest untouched.
     *
     * @param pageAddr  page address
     * @param itemId    item ID of the slot where row version (or its first fragment) is stored in this page
     * @param pageSize  size of the page
     * @param timestamp timestamp to store
     */
    public void updateTimestamp(long pageAddr, int itemId, int pageSize, @Nullable HybridTimestamp timestamp) {
        int payloadOffset = getPayloadOffset(pageAddr, itemId, pageSize, 0);

        HybridTimestamps.writeTimestampToMemory(pageAddr, payloadOffset + RowVersion.TIMESTAMP_OFFSET, timestamp);
    }

    /**
     * Updates next link leaving the rest untouched.
     *
     * @param pageAddr Page address.
     * @param itemId Item ID of the slot where row version (or its first fragment) is stored in this page.
     * @param pageSize Size of the page.
     * @param nextLink Next link to store.
     */
    public void updateNextLink(long pageAddr, int itemId, int pageSize, long nextLink) {
        int payloadOffset = getPayloadOffset(pageAddr, itemId, pageSize, 0);

        writePartitionless(pageAddr + payloadOffset + RowVersion.NEXT_LINK_OFFSET, nextLink);
    }

    @Override
    protected void printPage(long addr, int pageSize, IgniteStringBuilder sb) {
        sb.app("RowVersionDataIo [\n");
        printPageLayout(addr, pageSize, sb);
        sb.app("\n]");
    }
}
