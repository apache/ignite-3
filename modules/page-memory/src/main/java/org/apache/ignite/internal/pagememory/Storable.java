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

package org.apache.ignite.internal.pagememory;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;

/**
 * Simple interface for data, store in some RowStore.
 */
public interface Storable {
    /** Number of bytes a data type takes in storage. */
    int DATA_TYPE_SIZE_BYTES = 1;

    /** Offset of data type from the beginning of the row. */
    int DATA_TYPE_OFFSET = 0;

    /**
     * Sets link for this row.
     *
     * @param link Link for this row.
     */
    void link(long link);

    /**
     * Returns link for this row.
     */
    long link();

    /**
     * Return partition id.
     */
    int partition();

    /**
     * Returns row size (in bytes) in page.
     *
     * @throws IgniteInternalCheckedException If failed.
     */
    int size() throws IgniteInternalCheckedException;

    /**
     * Returns row header size (in bytes) in page.
     *
     * <p>Header is indivisible part of row which is entirely available on the very first page followed by the row link.
     */
    int headerSize();

    /**
     * Writes the row.
     *
     * @param pageAddr Page address.
     * @param dataOff Data offset.
     * @param payloadSize Payload size.
     * @param newRow {@code False} if existing cache entry is updated, in this case skip key data write.
     */
    void writeRowData(
            long pageAddr,
            int dataOff,
            int payloadSize,
            boolean newRow
    );

    /**
     * Writes row data fragment.
     *
     * @param pageBuf Byte buffer.
     * @param rowOff Offset in row data bytes.
     * @param payloadSize Data length that should be written in a fragment.
     */
    void writeFragmentData(
            ByteBuffer pageBuf,
            int rowOff,
            int payloadSize
    );

    static void putValueBufferIntoPage(ByteBuffer pageBuffer, ByteBuffer valueBuffer, int offset, int payloadSize) {
        int oldPosition = valueBuffer.position();
        int oldLimit = valueBuffer.limit();

        valueBuffer.position(offset);
        valueBuffer.limit(offset + payloadSize);
        pageBuffer.put(valueBuffer);
        valueBuffer.position(oldPosition);
        valueBuffer.limit(oldLimit);
    }
}
