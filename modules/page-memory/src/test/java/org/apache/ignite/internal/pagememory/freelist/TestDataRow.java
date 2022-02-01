package org.apache.ignite.internal.pagememory.freelist;

import static org.apache.ignite.internal.pagememory.freelist.TestDataPageIo.VERSIONS;

import org.apache.ignite.internal.pagememory.Storable;
import org.apache.ignite.internal.pagememory.io.AbstractDataPageIo;
import org.apache.ignite.internal.pagememory.io.IoVersions;

/**
 * Test storable row with raw data.
 */
class TestDataRow implements Storable {
    private long link;

    final byte[] bytes;

    /**
     * Constructor.
     *
     * @param size Size of the object in bytes.
     */
    TestDataRow(int size) {
        bytes = new byte[size];
    }

    /** {@inheritDoc} */
    @Override
    public void link(long link) {
        this.link = link;
    }

    /** {@inheritDoc} */
    @Override
    public long link() {
        return link;
    }

    /** {@inheritDoc} */
    @Override
    public int partition() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public int size() {
        return bytes.length;
    }

    /** {@inheritDoc} */
    @Override
    public int headerSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public IoVersions<? extends AbstractDataPageIo> ioVersions() {
        return VERSIONS;
    }
}
