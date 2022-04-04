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
import org.apache.ignite.internal.storage.SearchRow;

/**
 * {@link SearchRow} implementation.
 */
public class TableSearchRow {
    protected final int hash;

    protected final ByteBuffer key;

    /**
     * Constructor.
     *
     * @param hash Key hash.
     * @param key Key byte buffer.
     */
    public TableSearchRow(int hash, ByteBuffer key) {
        assert !key.isReadOnly();
        assert key.position() == 0;

        this.hash = hash;
        this.key = key;
    }

    /**
     * Returns key object as a byte buffer.
     */
    public ByteBuffer key() {
        return key.rewind();
    }

    /**
     * Returns hash of row.
     */
    public int hash() {
        return hash;
    }

    /**
     * Returns a row link.
     */
    public long link() {
        return 0;
    }
}
