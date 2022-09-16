/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.storage.basic;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.ignite.internal.storage.DataRow;

/**
 * Basic array-based implementation of the {@link DataRow}.
 */
public class SimpleDataRow implements DataRow {
    /** Key array. */
    private final byte[] key;

    /** Value array. */
    private final byte[] value;

    /**
     * Constructor.
     *
     * @param key   Key.
     * @param value Value.
     */
    public SimpleDataRow(byte[] key, byte[] value) {
        assert key != null;
        assert value != null;

        this.key = key;
        this.value = value;
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer key() {
        return ByteBuffer.wrap(key);
    }

    /** {@inheritDoc} */
    @Override
    public byte[] keyBytes() {
        return key;
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer value() {
        return ByteBuffer.wrap(value);
    }

    /** {@inheritDoc} */
    @Override
    public byte[] valueBytes() {
        return value;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DataRow)) {
            return false;
        }

        DataRow row = (DataRow) o;
        return Arrays.equals(key, row.keyBytes()) && Arrays.equals(value, row.valueBytes());
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }
}
