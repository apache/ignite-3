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
