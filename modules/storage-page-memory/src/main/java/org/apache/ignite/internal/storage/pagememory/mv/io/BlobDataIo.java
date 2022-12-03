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

import static org.apache.ignite.internal.pagememory.util.PageUtils.putBytes;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putInt;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putShort;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagememory.io.AbstractDataPageIo;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.storage.pagememory.mv.Blob;
import org.apache.ignite.lang.IgniteStringBuilder;

/**
 * Data pages IO for {@link Blob}.
 */
public class BlobDataIo extends AbstractDataPageIo<Blob> {
    /** Page IO type. */
    private static final short T_BLOB_DATA_IO = 13;

    /** I/O versions. */
    public static final IoVersions<BlobDataIo> VERSIONS = new IoVersions<>(new BlobDataIo(1));

    /**
     * Constructor.
     *
     * @param ver Page format version.
     */
    private BlobDataIo(int ver) {
        super(T_BLOB_DATA_IO, ver);
    }

    @Override
    protected void writeRowData(long pageAddr, int dataOff, int payloadSize, Blob blob, boolean newRow) {
        assertPageType(pageAddr);

        long addr = pageAddr + dataOff;

        putShort(addr, 0, narrowIntToShort(payloadSize));
        addr += Short.BYTES;

        putInt(addr, 0, blob.valueSize());
        addr += Integer.BYTES;

        putBytes(addr, 0, blob.value());
    }

    @Override
    protected void writeFragmentData(Blob blob, ByteBuffer pageBuf, int rowOff, int payloadSize) {
        assertPageType(pageBuf);

        if (rowOff == 0) {
            // first fragment
            assert blob.headerSize() <= payloadSize : "Header must entirely fit in the first fragment, but header size is "
                    + blob.headerSize() + " and payload size is " + payloadSize;

            pageBuf.putInt(blob.valueSize());

            pageBuf.put(blob.value(), 0, payloadSize - blob.headerSize());
        } else {
            // non-first fragment
            assert rowOff >= blob.headerSize();

            pageBuf.put(blob.value(), rowOff - blob.headerSize(), payloadSize);
        }
    }

    @Override
    protected void printPage(long addr, int pageSize, IgniteStringBuilder sb) {
        sb.app("BlobDataIo [\n");
        printPageLayout(addr, pageSize, sb);
        sb.app("\n]");
    }
}
