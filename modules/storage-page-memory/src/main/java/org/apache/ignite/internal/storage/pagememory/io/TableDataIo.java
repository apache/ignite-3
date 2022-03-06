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

package org.apache.ignite.internal.storage.pagememory.io;

import static org.apache.ignite.internal.pagememory.util.PageUtils.putBytes;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putShort;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagememory.io.AbstractDataPageIo;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.storage.pagememory.TableDataRow;
import org.apache.ignite.internal.storage.pagememory.TableTree;
import org.apache.ignite.lang.IgniteStringBuilder;

/**
 * Data pages IO for {@link TableTree}.
 */
// TODO: IGNITE-16666 Fragment storage optimization.
public class TableDataIo extends AbstractDataPageIo<TableDataRow> {
    /** Page IO type. */
    public static final short T_TABLE_DATA_IO = 6;

    /** I/O versions. */
    public static final IoVersions<TableDataIo> VERSIONS = new IoVersions<>(new TableDataIo(1));

    /**
     * Constructor.
     *
     * @param ver Page format version.
     */
    protected TableDataIo(int ver) {
        super(T_TABLE_DATA_IO, ver);
    }

    /** {@inheritDoc} */
    @Override
    protected void writeRowData(long pageAddr, int dataOff, int payloadSize, TableDataRow row, boolean newRow) {
        assertPageType(pageAddr);

        long addr = pageAddr + dataOff;

        if (newRow) {
            putShort(addr, 0, (short) payloadSize);
            addr += 2;

            byte[] keyBytes = row.keyBytes();

            putInt(addr, 0, keyBytes.length);
            addr += 4;

            putBytes(addr, 0, keyBytes);
            addr += keyBytes.length;
        } else {
            addr += 2 + 4 + row.keyBytes().length;
        }

        byte[] valueBytes = row.valueBytes();

        putInt(addr, 0, valueBytes.length);
        addr += 4;

        putBytes(addr, 0, valueBytes);
    }

    /** {@inheritDoc} */
    @Override
    protected void writeFragmentData(TableDataRow row, ByteBuffer buf, int rowOff, int payloadSize) {
        assertPageType(buf);

        byte[] keyBytes = row.keyBytes();

        int written = writeFragmentBytes(buf, rowOff, 0, payloadSize, keyBytes);

        written += writeFragmentBytes(buf, rowOff + written, 4 + keyBytes.length, payloadSize - written, row.valueBytes());

        assert written == payloadSize;
    }

    /** {@inheritDoc} */
    @Override
    protected void printPage(long addr, int pageSize, IgniteStringBuilder sb) {
        sb.app("TableDataIo [\n");
        printPageLayout(addr, pageSize, sb);
        sb.app("\n]");
    }

    /**
     * Try to write fragment data.
     *
     * @param buf Byte buffer to write to.
     * @param rowOff Offset in row data bytes.
     * @param expOff Expected offset in row data bytes.
     * @param payloadSize Data length that should be written in this fragment.
     * @param bytes Bytes to be written.
     * @return Actually written data, in bytes.
     */
    private int writeFragmentBytes(
            ByteBuffer buf,
            int rowOff,
            int expOff,
            int payloadSize,
            byte[] bytes
    ) {
        if (payloadSize == 0) {
            // No space left to write.
            return 0;
        }

        if (rowOff >= expOff + 4 + bytes.length) {
            // Already fully written to the buffer.
            return 0;
        }

        int len = Math.min(payloadSize, expOff + 4 + bytes.length - rowOff);

        putValue(buf, rowOff - expOff, len, bytes);

        return len;
    }

    private void putValue(
            ByteBuffer buf,
            int off,
            int len,
            byte[] bytes
    ) {
        if (off == 0 && len >= 4) {
            buf.putInt(bytes.length);

            len -= 4;
        } else if (off >= 4) {
            off -= 4;
        } else {
            // Partial length write.
            ByteBuffer tmp = ByteBuffer.allocate(4);

            tmp.order(buf.order());

            tmp.putInt(bytes.length);

            tmp.position(off);

            if (len < tmp.capacity()) {
                tmp.limit(off + Math.min(len, tmp.capacity() - off));
            }

            buf.put(tmp);

            if (tmp.limit() < 4) {
                return;
            }

            len -= 4 - off;
            off = 0;
        }

        buf.put(bytes, off, len);
    }
}
