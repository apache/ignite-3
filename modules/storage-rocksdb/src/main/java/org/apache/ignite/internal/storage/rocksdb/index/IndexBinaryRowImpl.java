/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage.rocksdb.index;

import java.util.Arrays;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.index.IndexBinaryRow;

/**
 * {@link IndexBinaryRow} implementation that uses {@link BinaryRow} serialization.
 */
class IndexBinaryRowImpl implements IndexBinaryRow {
    private final byte[] bytes;

    private final BinaryRow pk;

    IndexBinaryRowImpl(byte[] bytes, byte[] pkBytes) {
        this.bytes = bytes;
        this.pk = new ByteBufferRow(pkBytes);
    }

    IndexBinaryRowImpl(BinaryRow row, BinaryRow pk) {
        this.bytes = row.bytes();
        this.pk = pk;
    }

    @Override
    public byte[] rowBytes() {
        return bytes;
    }

    @Override
    public BinaryRow primaryKey() {
        return pk;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexBinaryRowImpl that = (IndexBinaryRowImpl) o;
        return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }
}
