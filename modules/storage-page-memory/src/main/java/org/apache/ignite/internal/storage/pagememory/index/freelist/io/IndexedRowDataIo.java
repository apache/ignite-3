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

package org.apache.ignite.internal.storage.pagememory.index.freelist.io;

import static org.apache.ignite.internal.pagememory.util.PageUtils.putByteBuffer;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putInt;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagememory.io.AbstractDataPageIo;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.storage.pagememory.index.IndexPageTypes;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexedRow;
import org.apache.ignite.lang.IgniteStringBuilder;

/**
 * Data pages IO for {@link IndexedRow}.
 */
public class IndexedRowDataIo extends AbstractDataPageIo<IndexedRow> {
    /** I/O versions. */
    public static final IoVersions<IndexedRowDataIo> VERSIONS = new IoVersions<>(new IndexedRowDataIo(1));

    /**
     * Constructor.
     *
     * @param ver Page format version.
     */
    protected IndexedRowDataIo(int ver) {
        super(IndexPageTypes.T_VALUE_VERSION_DATA_IO, ver);
    }

    /** {@inheritDoc} */
    @Override
    protected void writeRowData(long pageAddr, int dataOff, int payloadSize, IndexedRow row, boolean newRow) {
        assertPageType(pageAddr);

        putInt(pageAddr, dataOff + IndexedRow.SIZE_OFFSET, row.valueSize());

        putByteBuffer(pageAddr, dataOff + IndexedRow.VALUE_OFFSET, row.valueBuffer());
    }

    /** {@inheritDoc} */
    @Override
    protected void writeFragmentData(IndexedRow row, ByteBuffer pageBuf, int rowOff, int payloadSize) {
        assertPageType(pageBuf);

        if (rowOff == 0) {
            // First fragment.
            assert row.headerSize() <= payloadSize;

            pageBuf.putInt(row.valueSize());

            putValueBufferIntoPage(pageBuf, row.valueBuffer(), 0, payloadSize - IndexedRow.VALUE_OFFSET);
        } else {
            // Not a first fragment.
            assert rowOff >= row.headerSize();

            putValueBufferIntoPage(pageBuf, row.valueBuffer(), rowOff - IndexedRow.VALUE_OFFSET, payloadSize);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected void printPage(long addr, int pageSize, IgniteStringBuilder sb) {
        sb.app("IndexedRowDataIo [\n");
        printPageLayout(addr, pageSize, sb);
        sb.app("\n]");
    }
}
