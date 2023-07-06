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

package org.apache.ignite.internal.schema;

import java.nio.ByteBuffer;

/**
 * Binary row implementation that stores the schema version as a separate field.
 */
public class BinaryRowImpl implements BinaryRow {
    private final int schemaVersion;

    private final boolean hasValue;

    private final ByteBuffer binaryTuple;

    /**
     * Constructor.
     *
     * @param schemaVersion Schema version.
     * @param hasValue {@code true} if the {@code binaryTuple} contains both a key and a value, {@code false} if it only contains a key.
     * @param binaryTuple Binary tuple.
     */
    public BinaryRowImpl(int schemaVersion, boolean hasValue, ByteBuffer binaryTuple) {
        this.schemaVersion = schemaVersion;
        this.hasValue = hasValue;
        this.binaryTuple = binaryTuple;
    }

    @Override
    public int schemaVersion() {
        return schemaVersion;
    }

    @Override
    public boolean hasValue() {
        return hasValue;
    }

    @Override
    public ByteBuffer tupleSlice() {
        return binaryTuple.slice().order(ORDER);
    }

    @Override
    public ByteBuffer byteBuffer() {
        return ByteBuffer.allocate(length())
                .order(ORDER)
                .putShort((short) schemaVersion())
                .put((byte) (hasValue ? 1 : 0))
                .put(tupleSlice())
                .rewind();
    }

    @Override
    public int length() {
        return Short.BYTES + Byte.BYTES + binaryTuple.remaining();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BinaryRowImpl binaryRow = (BinaryRowImpl) o;

        if (schemaVersion != binaryRow.schemaVersion) {
            return false;
        }
        if (hasValue != binaryRow.hasValue) {
            return false;
        }
        return binaryTuple.equals(binaryRow.binaryTuple);
    }

    @Override
    public int hashCode() {
        int result = schemaVersion;
        result = 31 * result + (hasValue ? 1 : 0);
        result = 31 * result + binaryTuple.hashCode();
        return result;
    }
}
