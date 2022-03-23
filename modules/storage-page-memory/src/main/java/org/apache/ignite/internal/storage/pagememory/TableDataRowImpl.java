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

package org.apache.ignite.internal.storage.pagememory;

import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;
import static org.apache.ignite.internal.storage.StorageUtils.toByteArray;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagememory.io.AbstractDataPageIo;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.storage.pagememory.io.TableDataIo;

/**
 * {@link TableDataRow} implementation.
 */
// TODO: IGNITE-16666 Fragment storage optimization.
class TableDataRowImpl implements TableDataRow {
    private long link;

    private final int hash;

    private final ByteBuffer key;

    private final ByteBuffer value;

    /**
     * Constructor.
     *
     * @param link Row link.
     * @param hash Row hash.
     * @param key Key byte buffer.
     * @param value Value byte buffer.
     */
    TableDataRowImpl(long link, int hash, ByteBuffer key, ByteBuffer value) {
        assert !key.isReadOnly();
        assert key.position() == 0;
        assert !value.isReadOnly();
        assert value.position() == 0;

        this.link = link;
        this.hash = hash;

        this.key = key;
        this.value = value;
    }

    /**
     * Constructor.
     *
     * @param hash Row hash.
     * @param key Key byte buffer.
     * @param value Value byte buffer.
     */
    TableDataRowImpl(int hash, ByteBuffer key, ByteBuffer value) {
        this(0, hash, key, value);
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
        return 4 + key.limit() + 4 + value.limit();
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
        return toByteArray(value());
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer value() {
        return value.rewind();
    }

    /** {@inheritDoc} */
    @Override
    public byte[] keyBytes() {
        return toByteArray(key());
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer key() {
        return key.rewind();
    }

    /** {@inheritDoc} */
    @Override
    public int hash() {
        return hash;
    }
}
