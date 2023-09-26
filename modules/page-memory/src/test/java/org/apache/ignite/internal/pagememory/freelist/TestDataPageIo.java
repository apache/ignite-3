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

package org.apache.ignite.internal.pagememory.freelist;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.pagememory.io.AbstractDataPageIo;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.util.PageUtils;

/**
 * Test DataPageIo for {@link TestDataRow}.
 */
class TestDataPageIo extends AbstractDataPageIo<TestDataRow> {
    /** I/O versions. */
    static final IoVersions<TestDataPageIo> VERSIONS = new IoVersions<>(new TestDataPageIo());

    /**
     * Private constructor.
     */
    private TestDataPageIo() {
        super(1, 1);
    }

    /** {@inheritDoc} */
    @Override
    protected void writeRowData(long pageAddr, int dataOff, int payloadSize, TestDataRow row, boolean newRow) {
        assertPageType(pageAddr);

        long addr = pageAddr + dataOff;

        if (newRow) {
            PageUtils.putShort(addr, 0, (short) payloadSize);

            addr += 2;
        } else {
            addr += 2;
        }

        PageUtils.putBytes(addr, 0, row.bytes);
    }

    /** {@inheritDoc} */
    @Override
    protected void writeFragmentData(TestDataRow row, ByteBuffer pageBuf, int rowOff, int payloadSize) {
        assertPageType(pageBuf);

        if (payloadSize > 0) {
            pageBuf.put(row.bytes, rowOff, payloadSize);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected void printPage(long addr, int pageSize, IgniteStringBuilder sb) {
        sb.app("TestDataPageIo [\n");
        printPageLayout(addr, pageSize, sb);
        sb.app("\n]");
    }
}
