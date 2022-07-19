/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.tx.storage.state.inmemory;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagememory.io.AbstractDataPageIo;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteStringBuilder;

public class TxMetaStorageDataIo extends AbstractDataPageIo<TxMetaRowWrapper> {
    /** Page IO type. */
    public static final short T_TX_STORAGE_DATA_IO = 10004;

    /** I/O versions. */
    public static final IoVersions<TxMetaStorageDataIo> VERSIONS = new IoVersions<>(new TxMetaStorageDataIo(1));

    /**
     * Constructor.
     *
     * @param ver  Page format version.
     */
    protected TxMetaStorageDataIo(int ver) {
        super(T_TX_STORAGE_DATA_IO, ver);
    }

    @Override protected void writeFragmentData(
        TxMetaRowWrapper row,
        ByteBuffer buf,
        int rowOff,
        int payloadSize
    ) throws IgniteInternalCheckedException {

    }

    @Override protected void writeRowData(
        long pageAddr,
        int dataOff,
        int payloadSize,
        TxMetaRowWrapper row,
        boolean newRow
    ) throws IgniteInternalCheckedException {

    }

    @Override
    protected void printPage(long addr, int pageSize, IgniteStringBuilder sb) {
        sb.app("TxMetaStorageDataIo [\n");
        printPageLayout(addr, pageSize, sb);
        sb.app("\n]");
    }
}
