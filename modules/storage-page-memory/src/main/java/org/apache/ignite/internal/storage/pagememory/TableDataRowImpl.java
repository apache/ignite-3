package org.apache.ignite.internal.storage.pagememory;

import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagememory.io.AbstractDataPageIo;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.storage.pagememory.io.TableDataIo;

/**
 * {@link TableDataRow} implementation.
 */
class TableDataRowImpl implements TableDataRow {
    private long link;

    private final int hash;

    private final byte[] keyBytes;

    private final byte[] valueBytes;

    /**
     * Constructor.
     *
     * @param link Row link.
     * @param hash Row hash.
     * @param keyBytes Key bytes.
     * @param valueBytes Value bytes.
     */
    TableDataRowImpl(long link, int hash, byte[] keyBytes, byte[] valueBytes) {
        this.link = link;
        this.hash = hash;
        this.keyBytes = keyBytes;
        this.valueBytes = valueBytes;
    }

    /**
     * Constructor.
     *
     * @param hash Row hash.
     * @param keyBytes Key bytes.
     * @param valueBytes Value bytes.
     */
    TableDataRowImpl(int hash, byte[] keyBytes, byte[] valueBytes) {
        this.link = link;
        this.hash = hash;
        this.keyBytes = keyBytes;
        this.valueBytes = valueBytes;
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
        return partitionId(pageId(link));
    }

    /** {@inheritDoc} */
    @Override
    public int size() {
        return 4 + keyBytes.length + 4 + valueBytes.length;
    }

    /** {@inheritDoc} */
    @Override
    public int headerSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public IoVersions<? extends AbstractDataPageIo> ioVersions() {
        return TableDataIo.VERSIONS;
    }

    /** {@inheritDoc} */
    @Override
    public byte[] valueBytes() {
        return valueBytes;
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer value() {
        return ByteBuffer.wrap(valueBytes);
    }

    /** {@inheritDoc} */
    @Override
    public byte[] keyBytes() {
        return keyBytes;
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer key() {
        return ByteBuffer.wrap(keyBytes);
    }

    /** {@inheritDoc} */
    @Override
    public int hash() {
        return hash;
    }
}
