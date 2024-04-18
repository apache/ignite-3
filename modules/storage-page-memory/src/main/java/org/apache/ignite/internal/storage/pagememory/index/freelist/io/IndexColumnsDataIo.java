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

package org.apache.ignite.internal.storage.pagememory.index.freelist.io;

import static org.apache.ignite.internal.pagememory.util.PageUtils.putByteBuffer;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putShort;
import static org.apache.ignite.internal.storage.pagememory.index.IndexPageTypes.T_INDEX_COLUMNS_DATA_IO;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.pagememory.io.AbstractDataPageIo;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;

/**
 * Data pages IO for {@link IndexColumns}.
 */
public class IndexColumnsDataIo extends AbstractDataPageIo<IndexColumns> {
    /** I/O versions. */
    public static final IoVersions<IndexColumnsDataIo> VERSIONS = new IoVersions<>(new IndexColumnsDataIo(1));

    /**
     * Constructor.
     *
     * @param ver Page format version.
     */
    protected IndexColumnsDataIo(int ver) {
        super(T_INDEX_COLUMNS_DATA_IO, ver);
    }

    @Override
    protected void writeRowData(long pageAddr, int dataOff, int payloadSize, IndexColumns row, boolean newRow) {
        assertPageType(pageAddr);

        putShort(pageAddr, dataOff, (short) payloadSize);

        dataOff += Short.BYTES;

        putInt(pageAddr, dataOff + IndexColumns.SIZE_OFFSET, row.valueSize());

        putByteBuffer(pageAddr, dataOff + IndexColumns.VALUE_OFFSET, row.valueBuffer());
    }

    @Override
    protected void writeFragmentData(IndexColumns row, ByteBuffer pageBuf, int rowOff, int payloadSize) {
        assertPageType(pageBuf);

        if (rowOff == 0) {
            // First fragment.
            assert row.headerSize() <= payloadSize;

            pageBuf.putInt(row.valueSize());

            putValueBufferIntoPage(pageBuf, row.valueBuffer(), 0, payloadSize - IndexColumns.VALUE_OFFSET);
        } else {
            // Not a first fragment.
            assert rowOff >= row.headerSize();

            putValueBufferIntoPage(pageBuf, row.valueBuffer(), rowOff - IndexColumns.VALUE_OFFSET, payloadSize);
        }
    }

    @Override
    protected void printPage(long addr, int pageSize, IgniteStringBuilder sb) {
        sb.app("IndexColumnsDataIo [\n");
        printPageLayout(addr, pageSize, sb);
        sb.app("\n]");
    }
}
