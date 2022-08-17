/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import static org.apache.ignite.internal.storage.pagememory.mv.PartitionlessLinks.writePartitionlessLink;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagememory.io.AbstractDataPageIo;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.storage.pagememory.mv.PartitionlessLinks;
import org.apache.ignite.internal.storage.pagememory.mv.RowVersion;
import org.apache.ignite.internal.storage.pagememory.mv.Timestamps;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.lang.IgniteStringBuilder;
import org.jetbrains.annotations.Nullable;

/**
 * Data pages IO for {@link RowVersion}.
 */
public class RowVersionDataIo extends AbstractDataPageIo<RowVersion> {
    /** Page IO type. */
    public static final short T_VALUE_VERSION_DATA_IO = 12;

    /** I/O versions. */
    public static final IoVersions<RowVersionDataIo> VERSIONS = new IoVersions<>(new RowVersionDataIo(1));

    /**
     * Constructor.
     *
     * @param ver Page format version.
     */
    protected RowVersionDataIo(int ver) {
        super(T_VALUE_VERSION_DATA_IO, ver);
    }

    /** {@inheritDoc} */
    @Override
    protected void writeRowData(long pageAddr, int dataOff, int payloadSize, RowVersion row, boolean newRow) {
        assertPageType(pageAddr);

        long addr = pageAddr + dataOff;

        putShort(addr, 0, (short) payloadSize);
        addr += 2;

        addr += Timestamps.writeTimestampToMemory(addr, 0, row.timestamp());

        addr += writePartitionlessLink(addr, row.nextLink());

        putInt(addr, 0, row.valueSize());
        addr += 4;

        putByteBuffer(addr, 0, row.value());
    }

    /** {@inheritDoc} */
    @Override
    protected void writeFragmentData(RowVersion row, ByteBuffer buf, int rowOff, int payloadSize) {
        assertPageType(buf);

        if (rowOff == 0) {
            // first fragment
            assert row.headerSize() <= payloadSize : "Header must entirely fit in the first fragment, but header size is "
                    + row.headerSize() + " and payload size is " + payloadSize;

            Timestamps.writeTimestampToBuffer(buf, row.timestamp());

            PartitionlessLinks.writeToBuffer(buf, row.nextLink());

            buf.putInt(row.valueSize());

            int valueBytesToWrite = payloadSize - row.headerSize();
            putValueFragmentToBuffer(row, buf, 0, valueBytesToWrite);
        } else {
            // non-first fragment
            assert rowOff > row.headerSize();

            putValueFragmentToBuffer(row, buf, rowOff - row.headerSize(), payloadSize);
        }
    }

    private void putValueFragmentToBuffer(RowVersion row, ByteBuffer buf, int readBufferPosition, int valueBytesToWrite) {
        ByteBuffer valueBuffer = row.value();

        int oldLimit = valueBuffer.limit();
        int oldPosition = valueBuffer.position();

        valueBuffer.position(readBufferPosition);
        valueBuffer.limit(valueBuffer.position() + valueBytesToWrite);
        buf.put(valueBuffer);

        valueBuffer.position(oldPosition);
        valueBuffer.limit(oldLimit);
    }

    /**
     * Updates timestamp leaving the rest untouched.
     *
     * @param pageAddr  page address
     * @param itemId    item ID of the slot where row version (or its first fragment) is stored in this page
     * @param pageSize  size of the page
     * @param timestamp timestamp to store
     */
    public void updateTimestamp(long pageAddr, int itemId, int pageSize, @Nullable Timestamp timestamp) {
        int payloadOffset = getPayloadOffset(pageAddr, itemId, pageSize, 0);

        Timestamps.writeTimestampToMemory(pageAddr, payloadOffset + RowVersion.TIMESTAMP_OFFSET, timestamp);
    }

    /** {@inheritDoc} */
    @Override
    protected void printPage(long addr, int pageSize, IgniteStringBuilder sb) {
        sb.app("RowVersionDataIo [\n");
        printPageLayout(addr, pageSize, sb);
        sb.app("\n]");
    }
}
