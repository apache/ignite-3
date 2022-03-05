package org.apache.ignite.internal.storage.pagememory;

import java.nio.ByteBuffer;

/**
 * {@link TableSearchRow} implementation.
 */
class TableSearchRowImpl implements TableSearchRow {
    private final int hash;

    private final byte[] keyBytes;

    /**
     * Constructor.
     *
     * @param hash Key hash.
     * @param keyBytes Key bytes.
     */
    TableSearchRowImpl(int hash, byte[] keyBytes) {
        this.hash = hash;
        this.keyBytes = keyBytes;
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

    /** {@inheritDoc} */
    @Override
    public long link() {
        return 0;
    }
}
