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

import static org.apache.ignite.internal.pagememory.util.PageUtils.putByte;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putByteBuffer;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putShort;
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.writePartitionless;
import static org.apache.ignite.internal.storage.pagememory.mv.MvPageTypes.T_ROW_VERSION_DATA_IO;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.io.AbstractDataPageIo;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.util.PartitionlessLinks;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.pagememory.mv.HybridTimestamps;
import org.apache.ignite.internal.storage.pagememory.mv.RowVersion;
import org.apache.ignite.lang.IgniteStringBuilder;
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

        long addr = pageAddr + dataOff;

        putShort(addr, 0, (short) payloadSize);
        addr += Short.BYTES;

        addr += HybridTimestamps.writeTimestampToMemory(addr, 0, rowVersion.timestamp());

        addr += writePartitionless(addr, rowVersion.nextLink());

        putInt(addr, 0, rowVersion.valueSize());
        addr += Integer.BYTES;

        BinaryRow row = rowVersion.value();

        if (row != null) {
            // Write schema version in LE order.
            putByte(addr, 0, (byte) row.schemaVersion());
            putByte(addr, 1, (byte) (row.schemaVersion() >> Byte.SIZE));
            putByte(addr, 2, (byte) (row.hasValue() ? 1 : 0));
            putByteBuffer(addr, 3, row.tupleSlice());
        }
    }

    @Override
    protected void writeFragmentData(RowVersion rowVersion, ByteBuffer pageBuf, int rowOff, int payloadSize) {
        assertPageType(pageBuf);

        if (rowOff == 0) {
            // first fragment
            assert rowVersion.headerSize() <= payloadSize : "Header must entirely fit in the first fragment, but header size is "
                    + rowVersion.headerSize() + " and payload size is " + payloadSize;

            HybridTimestamps.writeTimestampToBuffer(pageBuf, rowVersion.timestamp());

            PartitionlessLinks.writeToBuffer(pageBuf, rowVersion.nextLink());

            pageBuf.putInt(rowVersion.valueSize());

            payloadSize -= rowVersion.headerSize();
        } else {
            // non-first fragment
            assert rowOff >= rowVersion.headerSize();

            rowOff -= rowVersion.headerSize();
        }

        BinaryRow row = rowVersion.value();

        if (payloadSize == 0 || row == null) {
            return;
        }

        writeFragmentedBinaryRow(row, pageBuf, rowOff, payloadSize);
    }

    private void writeFragmentedBinaryRow(BinaryRow row, ByteBuffer pageBuf, int offset, int payloadSize) {
        if (offset == 0) {
            if (payloadSize == 1) {
                // We only have space for the least significant byte.
                pageBuf.put((byte) row.schemaVersion());

                return;
            } else {
                pageBuf.put((byte) row.schemaVersion());
                pageBuf.put((byte) (row.schemaVersion() >> Byte.SIZE));

                payloadSize -= 2;
                offset += 2;
            }
        } else if (offset == 1) {
            // We could only write the least significant byte of the schema version during previous iteration.
            pageBuf.put((byte) (row.schemaVersion() >> Byte.SIZE));

            payloadSize -= 1;
            offset += 1;
        }

        if (payloadSize == 0) {
            return;
        }

        if (offset == 2) {
            pageBuf.put((byte) (row.hasValue() ? 1 : 0));

            payloadSize -= 1;
            offset += 1;

            if (payloadSize == 0) {
                return;
            }
        }

        putValueBufferIntoPage(pageBuf, row.tupleSlice(), offset - Short.BYTES - Byte.BYTES, payloadSize);
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
