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

package org.apache.ignite.internal.storage.pagememory.index.hash;

import static org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns.VALUE_OFFSET;
import static org.apache.ignite.internal.util.GridUnsafe.wrapPointer;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.datapage.PageMemoryTraversal;
import org.apache.ignite.internal.pagememory.io.DataPagePayload;

/**
 * Compares the index columns from {@link PageMemory} with the lookup index columns.
 */
public class CompareIndexColumnsValue implements PageMemoryTraversal<ByteBuffer> {
    private int cmp;

    private int pos;

    @Override
    public long consumePagePayload(long link, long pageAddr, DataPagePayload payload, ByteBuffer other) {
        ByteBuffer buffer;

        if (pos == 0) {
            // First fragment.
            buffer = wrapPointer(pageAddr + payload.offset() + VALUE_OFFSET, payload.payloadSize() - VALUE_OFFSET);
        } else {
            buffer = wrapPointer(pageAddr + payload.offset(), payload.payloadSize());
        }

        int oldLimit = other.limit();

        cmp = buffer.compareTo(other.position(pos).limit(Math.min(oldLimit, pos + buffer.limit())));

        other.limit(oldLimit);

        pos += buffer.limit();

        return cmp != 0 || !payload.hasMoreFragments() ? STOP_TRAVERSAL : payload.nextLink();
    }

    /**
     * Returns compare result.
     */
    public int compareResult() {
        return cmp;
    }
}
