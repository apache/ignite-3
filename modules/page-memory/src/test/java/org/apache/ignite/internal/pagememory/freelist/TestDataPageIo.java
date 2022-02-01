package org.apache.ignite.internal.pagememory.freelist;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagememory.io.AbstractDataPageIo;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.lang.IgniteStringBuilder;

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
        super(T_DATA, 1);
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
    protected void writeFragmentData(TestDataRow row, ByteBuffer buf, int rowOff, int payloadSize) {
        assertPageType(buf);

        if (payloadSize > 0) {
            buf.put(row.bytes, rowOff, payloadSize);
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
