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

package org.apache.ignite.internal.storage.pagememory.mv;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagememory.Storable;
import org.apache.ignite.internal.pagememory.io.AbstractDataPageIo;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.storage.pagememory.mv.io.RowVersionDataIo;
import org.apache.ignite.internal.tx.Timestamp;
import org.jetbrains.annotations.Nullable;

/**
 * Represents row version inside row version chain.
 */
public class RowVersion implements Storable {
    /**
     * A 'timestamp' representing absense of a timestamp.
     */
    public static final Timestamp NULL_TIMESTAMP = new Timestamp(Long.MIN_VALUE, Long.MIN_VALUE);
    /**
     * Represents an absent partitionless link.
     */
    public static final long NULL_LINK = 0;

    private static final int TIMESTAMP_STORE_SIZE_BYTES = 2 * Long.BYTES;
    private static final int NEXT_LINK_STORE_SIZE_BYTES = PartitionlessLinks.PARTITIONLESS_LINK_SIZE_BYTES;

    private final int partitionId;
    private long link;

    @Nullable
    private final Timestamp timestamp;
    private final long nextLink;
    private final int valueSize;
    private final ByteBuffer value;

    /**
     * Constructor.
     */
    public RowVersion(int partitionId, long nextLink, ByteBuffer value) {
        this(partitionId, 0, null, nextLink, value);
    }

    /**
     * Constructor.
     */
    public RowVersion(int partitionId, long link, @Nullable Timestamp timestamp, long nextLink, ByteBuffer value) {
        this.partitionId = partitionId;
        link(link);

        assert !NULL_TIMESTAMP.equals(timestamp) : "Null timestamp provided";

        this.timestamp = timestamp;
        this.nextLink = nextLink;
        this.valueSize = value.limit();
        this.value = value;
    }

    @Nullable
    public Timestamp timestamp() {
        return timestamp;
    }

    public Timestamp timestampForStorage() {
        return timestamp == null ? NULL_TIMESTAMP : timestamp;
    }

    /**
     * Returns partitionless link of the next version or {@code 0} if this version is the last in the chain (i.e. it's the oldest version).
     *
     * @return partitionless link of the next version or {@code 0} if this version is the last in the chain
     */
    public long nextLink() {
        return nextLink;
    }

    public int valueSize() {
        return valueSize;
    }

    public ByteBuffer value() {
        return value;
    }

    public boolean hasNextLink() {
        return nextLink != NULL_LINK;
    }

    boolean isTombstone() {
        return valueSize() == 0;
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
        return TIMESTAMP_STORE_SIZE_BYTES + NEXT_LINK_STORE_SIZE_BYTES + valueStoreSize();
    }

    private int valueStoreSize() {
        return Integer.BYTES + value.limit();
    }

    @Override
    public int headerSize() {
        return 0;
    }

    @Override
    public IoVersions<? extends AbstractDataPageIo> ioVersions() {
        return RowVersionDataIo.VERSIONS;
    }
}
