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
import java.util.Objects;
import org.apache.ignite.internal.pagememory.Storable;
import org.apache.ignite.internal.pagememory.io.AbstractDataPageIo;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.storage.pagememory.mv.io.RowVersionDataIo;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.tx.Timestamp;
import org.jetbrains.annotations.Nullable;

/**
 * Represents row version inside row version chain.
 */
public class RowVersion implements Storable {
    /** A 'timestamp' representing absense of a timestamp. */
    public static final Timestamp NULL_TIMESTAMP = new Timestamp(Long.MIN_VALUE, Long.MIN_VALUE);

    /** Represents an absent partitionless link. */
    public static final long NULL_LINK = 0;

    private static final int TIMESTAMP_STORE_SIZE_BYTES = 2 * Long.BYTES;
    private static final int NEXT_LINK_STORE_SIZE_BYTES = PartitionlessLinks.PARTITIONLESS_LINK_SIZE_BYTES;
    private static final int VALUE_SIZE_STORE_SIZE_BYTES = Integer.BYTES;

    public static final int TIMESTAMP_OFFSET = 0;
    public static final int NEXT_LINK_OFFSET = TIMESTAMP_STORE_SIZE_BYTES;
    public static final int VALUE_SIZE_OFFSET = NEXT_LINK_OFFSET + NEXT_LINK_STORE_SIZE_BYTES;
    public static final int VALUE_OFFSET = VALUE_SIZE_OFFSET + VALUE_SIZE_STORE_SIZE_BYTES;

    private final int partitionId;

    private long link;

    private final @Nullable Timestamp timestamp;

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
    public RowVersion(int partitionId, long link, @Nullable Timestamp timestamp, long nextLink, @Nullable ByteBuffer value) {
        this.partitionId = partitionId;
        link(link);

        assert !NULL_TIMESTAMP.equals(timestamp) : "Null timestamp provided";

        this.timestamp = timestamp;
        this.nextLink = nextLink;
        this.valueSize = value == null ? -1 : value.limit();
        this.value = value;
    }

    public @Nullable Timestamp timestamp() {
        return timestamp;
    }

    public Timestamp timestampForStorage() {
        return timestampForStorage(timestamp);
    }

    static Timestamp timestampForStorage(@Nullable Timestamp timestamp) {
        return timestamp == null ? NULL_TIMESTAMP : timestamp;
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

    public ByteBuffer value() {
        return Objects.requireNonNull(value);
    }

    public boolean hasNextLink() {
        return nextLink != NULL_LINK;
    }

    boolean isTombstone() {
        return isTombstone(valueSize());
    }

    static boolean isTombstone(int valueSize) {
        return valueSize == 0;
    }

    static boolean isTombstone(byte[] valueBytes) {
        return isTombstone(valueBytes.length);
    }

    boolean isUncommitted() {
        return isUncommitted(timestamp);
    }

    static boolean isUncommitted(Timestamp timestamp) {
        return timestamp == null;
    }

    boolean isCommitted() {
        return timestamp != null;
    }

    /** {@inheritDoc} */
    @Override
    public final void link(long link) {
        this.link = link;
    }

    /** {@inheritDoc} */
    @Override
    public final long link() {
        return link;
    }

    /** {@inheritDoc} */
    @Override
    public final int partition() {
        return partitionId;
    }

    /** {@inheritDoc} */
    @Override
    public int size() {
        assert value != null;

        return TIMESTAMP_STORE_SIZE_BYTES + NEXT_LINK_STORE_SIZE_BYTES + VALUE_SIZE_STORE_SIZE_BYTES + value.limit();
    }

    /** {@inheritDoc} */
    @Override
    public int headerSize() {
        return TIMESTAMP_STORE_SIZE_BYTES + NEXT_LINK_STORE_SIZE_BYTES + VALUE_SIZE_STORE_SIZE_BYTES;
    }

    /** {@inheritDoc} */
    @Override
    public IoVersions<? extends AbstractDataPageIo<?>> ioVersions() {
        return RowVersionDataIo.VERSIONS;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(RowVersion.class, this);
    }
}
