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

import static org.apache.ignite.internal.storage.StorageUtils.toByteArray;

import java.nio.ByteBuffer;

/**
 * {@link TableSearchRow} implementation.
 */
class TableSearchRowImpl implements TableSearchRow {
    private final int hash;

    private final ByteBuffer key;

    /**
     * Constructor.
     *
     * @param hash Key hash.
     * @param key Key byte buffer.
     */
    TableSearchRowImpl(int hash, ByteBuffer key) {
        assert !key.isReadOnly();
        assert key.position() == 0;

        this.hash = hash;
        this.key = key;
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

    /** {@inheritDoc} */
    @Override
    public long link() {
        return 0;
    }
}
