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

import static org.apache.ignite.internal.pagememory.util.PageUtils.putByteBuffer;
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

            ByteBuffer key = row.key();

            putInt(addr, 0, key.limit());
            addr += 4;

            putByteBuffer(addr, 0, key);
            addr += key.limit();
        } else {
            addr += 2 + 4 + row.key().limit();
        }

        ByteBuffer value = row.value();

        putInt(addr, 0, value.limit());
        addr += 4;

        putByteBuffer(addr, 0, value);
    }

    /** {@inheritDoc} */
    @Override
    protected void writeFragmentData(TableDataRow row, ByteBuffer buf, int rowOff, int payloadSize) {
        assertPageType(buf);

        ByteBuffer key = row.key();

        int written = writeFragmentByteBuffer(buf, rowOff, 0, payloadSize, key);

        written += writeFragmentByteBuffer(buf, rowOff + written, 4 + key.limit(), payloadSize - written, row.value());

        assert written == payloadSize;
    }

    /** {@inheritDoc} */
    @Override
    protected void printPage(long addr, int pageSize, IgniteStringBuilder sb) {
        sb.app("TableDataIo [\n");
        printPageLayout(addr, pageSize, sb);
        sb.app("\n]");
    }

    private int writeFragmentByteBuffer(
            ByteBuffer bufWriteTo,
            int rowOff,
            int expOff,
            int payloadSize,
            ByteBuffer bufReadFrom
    ) {
        if (payloadSize == 0) {
            // No space left to write.
            return 0;
        }

        if (rowOff >= expOff + 4 + bufReadFrom.limit()) {
            // Already fully written to the buffer.
            return 0;
        }

        int len = Math.min(payloadSize, expOff + 4 + bufReadFrom.limit() - rowOff);

        putValue(bufWriteTo, rowOff - expOff, len, bufReadFrom);

        return len;
    }

    private void putValue(
            ByteBuffer bufWriteTo,
            int off,
            int len,
            ByteBuffer bufReadFrom
    ) {
        if (off == 0 && len >= 4) {
            bufWriteTo.putInt(bufReadFrom.limit());

            len -= 4;
        } else if (off >= 4) {
            off -= 4;
        } else {
            // Partial length write.
            ByteBuffer tmp = ByteBuffer.allocate(4);

            tmp.order(bufWriteTo.order());

            tmp.putInt(bufReadFrom.limit());

            tmp.position(off);

            if (len < tmp.capacity()) {
                tmp.limit(off + Math.min(len, tmp.capacity() - off));
            }

            bufWriteTo.put(tmp);

            if (tmp.limit() < 4) {
                return;
            }

            len -= 4 - off;
            off = 0;
        }

        int oldBufLimit = bufReadFrom.limit();

        bufReadFrom.position(off);

        if (len < bufReadFrom.capacity()) {
            bufReadFrom.limit(off + Math.min(len, bufReadFrom.capacity() - off));
        }

        bufWriteTo.put(bufReadFrom);

        bufReadFrom.limit(oldBufLimit);
    }
}
