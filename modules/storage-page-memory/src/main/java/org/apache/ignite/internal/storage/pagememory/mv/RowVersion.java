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

package org.apache.ignite.internal.storage.pagememory.mv;

import static org.apache.ignite.internal.hlc.HybridTimestamp.HYBRID_TIMESTAMP_SIZE;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.NULL_LINK;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.Storable;
import org.apache.ignite.internal.pagememory.io.AbstractDataPageIo;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.util.PartitionlessLinks;
import org.apache.ignite.internal.storage.pagememory.mv.io.RowVersionDataIo;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Represents row version inside row version chain.
 */
public final class RowVersion implements Storable {
    private static final int NEXT_LINK_STORE_SIZE_BYTES = PartitionlessLinks.PARTITIONLESS_LINK_SIZE_BYTES;
    private static final int VALUE_SIZE_STORE_SIZE_BYTES = Integer.BYTES;

    public static final int TIMESTAMP_OFFSET = 0;
    public static final int NEXT_LINK_OFFSET = TIMESTAMP_OFFSET + HYBRID_TIMESTAMP_SIZE;
    public static final int VALUE_SIZE_OFFSET = NEXT_LINK_OFFSET + NEXT_LINK_STORE_SIZE_BYTES;
    public static final int VALUE_OFFSET = VALUE_SIZE_OFFSET + VALUE_SIZE_STORE_SIZE_BYTES;

    private final int partitionId;

    private long link;

    private final @Nullable HybridTimestamp timestamp;

    private final long nextLink;

    private final int valueSize;

    @IgniteToStringExclude
    private final @Nullable ByteBuffer value;

    /**
     * Constructor.
     */
    public RowVersion(int partitionId, long nextLink, ByteBuffer value) {
        this(partitionId, 0, null, nextLink, value);
    }

    /**
     * Constructor.
     */
    public RowVersion(int partitionId, HybridTimestamp commitTimestamp, long nextLink, ByteBuffer value) {
        this(partitionId, 0, commitTimestamp, nextLink, value);
    }

    /**
     * Constructor.
     */
    public RowVersion(int partitionId, long link, @Nullable HybridTimestamp timestamp, long nextLink, @Nullable ByteBuffer value) {
        this.partitionId = partitionId;
        link(link);

        this.timestamp = timestamp;
        this.nextLink = nextLink;
        this.valueSize = value == null ? -1 : value.limit();
        this.value = value;
    }

    /**
     * Constructor.
     */
    public RowVersion(int partitionId, long link, @Nullable HybridTimestamp timestamp, long nextLink, int valueSize) {
        this.partitionId = partitionId;
        link(link);

        this.timestamp = timestamp;
        this.nextLink = nextLink;
        this.valueSize = valueSize;
        this.value = null;
    }

    public @Nullable HybridTimestamp timestamp() {
        return timestamp;
    }

    /**
     * Returns partitionless link of the next version or {@code 0} if this version is the last in the chain (i.e. it's the oldest version).
     */
    public long nextLink() {
        return nextLink;
    }

    public int valueSize() {
        return valueSize;
    }

    public @Nullable ByteBuffer value() {
        return value;
    }

    public boolean hasNextLink() {
        return nextLink != NULL_LINK;
    }

    boolean isTombstone() {
        return isTombstone(valueSize());
    }

    static boolean isTombstone(int valueSize) {
        return valueSize <= 0;
    }

    static boolean isTombstone(byte[] valueBytes) {
        return isTombstone(valueBytes.length);
    }

    boolean isUncommitted() {
        return timestamp == null;
    }

    boolean isCommitted() {
        return timestamp != null;
    }

    @Override
    public final void link(long link) {
        this.link = link;
    }

    @Override
    public final long link() {
        return link;
    }

    @Override
    public final int partition() {
        return partitionId;
    }

    @Override
    public int size() {
        assert value != null;

        return headerSize() + value.limit();
    }

    @Override
    public int headerSize() {
        return HYBRID_TIMESTAMP_SIZE + NEXT_LINK_STORE_SIZE_BYTES + VALUE_SIZE_STORE_SIZE_BYTES;
    }

    @Override
    public IoVersions<? extends AbstractDataPageIo<?>> ioVersions() {
        return RowVersionDataIo.VERSIONS;
    }

    @Override
    public String toString() {
        return S.toString(RowVersion.class, this);
    }
}
