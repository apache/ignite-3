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

package org.apache.ignite.internal.storage.pagememory.index.freelist;

import static org.apache.ignite.internal.pagememory.util.PageIdUtils.NULL_LINK;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.Storable;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Index columns to store in free list.
 */
public class IndexColumns implements Storable {
    public static final byte DATA_TYPE = 0;

    /** Size offset. */
    public static final int SIZE_OFFSET = DATA_TYPE_OFFSET + DATA_TYPE_SIZE_BYTES;

    /** Value offset. Value goes right after the size. */
    public static final int VALUE_OFFSET = SIZE_OFFSET + Integer.BYTES;

    /** Partition ID. */
    private final int partitionId;

    /** Link value. */
    private long link = NULL_LINK;

    /** Byte buffer with binary tuple data. */
    private final @Nullable ByteBuffer valueBuffer;

    /**
     * Constructor.
     *
     * @param partitionId Partition ID.
     * @param valueBuffer Value buffer.
     */
    public IndexColumns(int partitionId, @Nullable ByteBuffer valueBuffer) {
        this.partitionId = partitionId;
        this.valueBuffer = valueBuffer;
    }

    /**
     * Constructor.
     *
     * @param partitionId Partition ID.
     * @param link Link.
     * @param valueBuffer Value buffer.
     */
    public IndexColumns(int partitionId, long link, @Nullable ByteBuffer valueBuffer) {
        this.partitionId = partitionId;
        this.link = link;
        this.valueBuffer = valueBuffer;
    }

    /**
     * Returns the size of binary tuple.
     */
    public int valueSize() {
        assert valueBuffer != null;

        return valueBuffer.limit();
    }

    /**
     * Returns a byte buffer that contains binary tuple data.
     */
    public ByteBuffer valueBuffer() {
        return valueBuffer;
    }

    @Override
    public void link(long link) {
        this.link = link;
    }

    @Override
    public long link() {
        return link;
    }

    @Override
    public int partition() {
        return partitionId;
    }

    @Override
    public int size() throws IgniteInternalCheckedException {
        return VALUE_OFFSET + valueSize();
    }

    @Override
    public int headerSize() {
        // Size of the tuple and its header. For further use in future optimizations.
        return VALUE_OFFSET + Byte.BYTES;
    }

    @Override
    public void writeRowData(long pageAddr, int dataOff, int payloadSize, boolean newRow) {
        PageUtils.putShort(pageAddr, dataOff, (short) payloadSize);

        dataOff += Short.BYTES;

        PageUtils.putByte(pageAddr, dataOff + DATA_TYPE_OFFSET, DATA_TYPE);

        PageUtils.putInt(pageAddr, dataOff + SIZE_OFFSET, valueSize());

        PageUtils.putByteBuffer(pageAddr, dataOff + VALUE_OFFSET, valueBuffer());
    }

    @Override
    public void writeFragmentData(
            ByteBuffer pageBuf,
            int rowOff,
            int payloadSize
    ) {
        if (rowOff == 0) {
            // First fragment.
            assert headerSize() <= payloadSize;

            pageBuf.put(DATA_TYPE);

            pageBuf.putInt(valueSize());

            Storable.putValueBufferIntoPage(pageBuf, valueBuffer, 0, valueSize());
        } else {
            // Not a first fragment.
            assert rowOff >= headerSize();

            Storable.putValueBufferIntoPage(pageBuf, valueBuffer, rowOff - VALUE_OFFSET, valueSize());
        }
    }
}
