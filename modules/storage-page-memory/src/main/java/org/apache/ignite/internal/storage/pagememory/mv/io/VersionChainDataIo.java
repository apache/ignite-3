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

import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putShort;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.internal.pagememory.io.AbstractDataPageIo;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.storage.pagememory.mv.PartitionlessLinks;
import org.apache.ignite.internal.storage.pagememory.mv.VersionChain;
import org.apache.ignite.lang.IgniteStringBuilder;

/**
 * {@link AbstractDataPageIo} for {@link VersionChain} instances.
 */
public class VersionChainDataIo extends AbstractDataPageIo<VersionChain> {
    /** Page IO type. */
    public static final short T_VERSION_CHAIN_IO = 7;

    /** I/O versions. */
    public static final IoVersions<VersionChainDataIo> VERSIONS = new IoVersions<>(new VersionChainDataIo(1));

    /**
     * Constructor.
     *
     * @param ver  Page format version.
     */
    protected VersionChainDataIo(int ver) {
        super(T_VERSION_CHAIN_IO, ver);
    }

    @Override
    protected void printPage(long addr, int pageSize, IgniteStringBuilder sb) {
        sb.app("VersionChainDataIo [\n");
        printPageLayout(addr, pageSize, sb);
        sb.app("\n]");
    }

    /** {@inheritDoc} */
    @Override
    protected void writeRowData(long pageAddr, int dataOff, int payloadSize, VersionChain row, boolean newRow) {
        assertPageType(pageAddr);

        long addr = pageAddr + dataOff;

        putShort(addr, 0, (short) payloadSize);
        addr += Short.BYTES;

        UUID txId = row.transactionId();
        long txIdHigh;
        long txIdLow;
        if (txId != null) {
            txIdHigh = txId.getMostSignificantBits();
            txIdLow = txId.getLeastSignificantBits();
        } else {
            txIdHigh = VersionChain.NULL_UUID_COMPONENT;
            txIdLow = VersionChain.NULL_UUID_COMPONENT;
        }

        putLong(addr, 0, txIdHigh);
        addr += Long.BYTES;

        putLong(addr, 0, txIdLow);
        addr += Long.BYTES;

        addr += PartitionlessLinks.writeToMemory(addr, row.headLink());
    }

    /** {@inheritDoc} */
    @Override
    protected void writeFragmentData(VersionChain row, ByteBuffer buf, int rowOff, int payloadSize) {
        assertPageType(buf);

        throw new UnsupportedOperationException("Splitting version chain rows to fragments is ridiculous!");
    }
}
