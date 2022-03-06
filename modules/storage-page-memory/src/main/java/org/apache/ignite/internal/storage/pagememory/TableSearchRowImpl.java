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
