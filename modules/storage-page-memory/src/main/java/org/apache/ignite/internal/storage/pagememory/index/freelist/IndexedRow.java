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

package org.apache.ignite.internal.storage.pagememory.index.freelist;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagememory.Storable;
import org.apache.ignite.internal.pagememory.io.AbstractDataPageIo;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.storage.pagememory.index.freelist.io.IndexedRowDataIo;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Indexed row to store in free list.
 */
public class IndexedRow implements Storable {
    /** Size offset. */
    public static final int SIZE_OFFSET = 0;

    /** Value offset. Value goes right after the size. */
    public static final int VALUE_OFFSET = SIZE_OFFSET + Integer.BYTES;

    /** Partition ID. */
    private final int partitionId;

    /** Link value. */
    private long link;

    /** Byte buffer with binary tuple data. */
    private final @Nullable ByteBuffer valueBuffer;

    public IndexedRow(int partitionId, @Nullable ByteBuffer valueBuffer) {
        this.partitionId = partitionId;
        this.valueBuffer = valueBuffer;
    }

    public IndexedRow(int partitionId, long link, @Nullable ByteBuffer valueBuffer) {
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
        return VALUE_OFFSET + Byte.BYTES;
    }

    @Override
    public IoVersions<? extends AbstractDataPageIo<?>> ioVersions() {
        return IndexedRowDataIo.VERSIONS;
    }
}
