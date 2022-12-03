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

import java.util.Objects;
import org.apache.ignite.internal.pagememory.Storable;
import org.apache.ignite.internal.pagememory.io.AbstractDataPageIo;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.storage.pagememory.mv.io.BlobDataIo;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;

/**
 * Represents some blob (just an array of bytes).
 */
public final class Blob implements Storable {
    private static final int VALUE_SIZE_STORE_SIZE_BYTES = Integer.BYTES;

    public static final int VALUE_SIZE_OFFSET = 0;
    public static final int VALUE_OFFSET = VALUE_SIZE_OFFSET + VALUE_SIZE_STORE_SIZE_BYTES;

    private final int partitionId;

    private long link;

    private final int valueSize;

    @IgniteToStringExclude
    private final byte[] value;

    /**
     * Constructor.
     */
    public Blob(int partitionId, byte[] value) {
        this(partitionId, 0, value);
    }

    /**
     * Constructor.
     */
    public Blob(int partitionId, long link, byte[] value) {
        this.partitionId = partitionId;
        link(link);

        this.valueSize = value.length;
        this.value = value;
    }

    public int valueSize() {
        return valueSize;
    }

    public byte[] value() {
        return Objects.requireNonNull(value);
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
    public int partition() {
        return partitionId;
    }

    /** {@inheritDoc} */
    @Override
    public int size() {
        assert value != null;

        return headerSize() + value.length;
    }

    /** {@inheritDoc} */
    @Override
    public int headerSize() {
        return VALUE_SIZE_STORE_SIZE_BYTES;
    }

    /** {@inheritDoc} */
    @Override
    public IoVersions<? extends AbstractDataPageIo<?>> ioVersions() {
        return BlobDataIo.VERSIONS;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(Blob.class, this);
    }
}
