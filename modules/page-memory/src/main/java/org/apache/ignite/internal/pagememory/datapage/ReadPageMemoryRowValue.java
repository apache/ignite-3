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

package org.apache.ignite.internal.pagememory.datapage;

import java.util.Objects;
import org.apache.ignite.internal.pagememory.Storable;
import org.apache.ignite.internal.pagememory.io.DataPagePayload;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Reads full data payload value (as byte array) from page memory. Supports fragmented values occupying more than one slot.
 *
 * <p>This works for the cases when the following conditions are satisfied:
 * 1. Row data starts with a fixed-length header, which is followed by the 'value'; the 'value' ends the row data
 * 2. Row data header contains 'value' size as an int at a fixed offset known beforehand
 * 3. The beginning of the header, including the 'value' size, is always stored in the first slot (i.e.
 * {@link Storable#headerSize()} is enough to include 'value' size
 */
public abstract class ReadPageMemoryRowValue implements PageMemoryTraversal<Void> {
    /**
     * First it's {@code true} (this means that we traverse first slots of versions of the Version Chain using NextLink);
     * then it's {@code false} (when we found the version we need and we read its value).
     */
    private boolean readingFirstSlot = true;

    private int valueSize;
    /**
     * Used to collect all the bytes of the target version value.
     */
    private byte @Nullable [] allValueBytes;
    /**
     * Number of bytes written to {@link #allValueBytes}.
     */
    private int transferredBytes = 0;

    @Override
    public long consumePagePayload(long link, long pageAddr, DataPagePayload payload, Void ignoredArg) {
        if (readingFirstSlot) {
            readingFirstSlot = false;
            return readFullyOrStartReadingFragmented(pageAddr, payload);
        } else {
            // We are continuing reading a fragmented row.
            return readNextFragment(pageAddr, payload);
        }
    }

    private long readFullyOrStartReadingFragmented(long pageAddr, DataPagePayload payload) {
        assert PageUtils.getByte(pageAddr, payload.offset() + Storable.DATA_TYPE_OFFSET) == dataType();

        valueSize = readValueSize(pageAddr, payload);

        if (!payload.hasMoreFragments()) {
            return readFully(pageAddr, payload);
        } else {
            allValueBytes = new byte[valueSize];
            transferredBytes = 0;

            readValueFragmentToArray(pageAddr, payload, valueOffsetInFirstSlot());

            return payload.nextLink();
        }
    }

    protected abstract byte dataType();

    private int readValueSize(long pageAddr, DataPagePayload payload) {
        return PageUtils.getInt(pageAddr, payload.offset() + valueSizeOffsetInFirstSlot());
    }

    private long readFully(long pageAddr, DataPagePayload payload) {
        allValueBytes = PageUtils.getBytes(pageAddr, payload.offset() + valueOffsetInFirstSlot(), valueSize);

        return STOP_TRAVERSAL;
    }

    private void readValueFragmentToArray(long pageAddr, DataPagePayload payload, int offsetToValue) {
        assert allValueBytes != null;

        int valueBytesToRead = payload.payloadSize() - offsetToValue;

        PageUtils.getBytes(pageAddr, payload.offset() + offsetToValue, allValueBytes, transferredBytes, valueBytesToRead);
        transferredBytes += valueBytesToRead;
    }

    private long readNextFragment(long pageAddr, DataPagePayload payload) {
        assert allValueBytes != null;

        readValueFragmentToArray(pageAddr, payload, 0);

        if (payload.hasMoreFragments()) {
            return payload.nextLink();
        } else {
            return STOP_TRAVERSAL;
        }
    }

    /**
     * Returns a byte array containing all the data of the page memory row in question. Should only be called after
     * the traversal has stopped.
     *
     * @return a byte array containing all the data of the page memory row in question
     */
    public byte[] result() {
        return Objects.requireNonNull(allValueBytes);
    }

    /**
     * Memory offset into first slot at which the 'value' size is stored (as int).
     *
     * @return offset into first slot at which the 'value' size is stored (as int)
     */
    protected abstract int valueSizeOffsetInFirstSlot();

    /**
     * Memory offset into first slot at which the 'value' starts.
     *
     * @return offset into first slot at which the 'value' starts
     */
    protected abstract int valueOffsetInFirstSlot();

    /**
     * Resets the object to make it ready for use.
     */
    public void reset() {
        readingFirstSlot = true;
        allValueBytes = null;
        transferredBytes = 0;
    }
}
