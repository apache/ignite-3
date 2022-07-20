/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.tx.storage.state.inmemory;

import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.pagememory.Storable;
import org.apache.ignite.internal.pagememory.io.AbstractDataPageIo;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.NotNull;

public class TxMetaRowWrapper implements Comparable<TxMetaRowWrapper>, Storable {
    private final UUID txId;

    private final TxMeta txMeta;

    private long link;

    private final int partition;

    public TxMetaRowWrapper(UUID txId, TxMeta txMeta, int partition) {
        this(txId, txMeta, 0, partition);
    }

    public TxMetaRowWrapper(UUID txId, TxMeta txMeta, long link, int partition) {
        this.txId = txId;
        this.txMeta = txMeta;
        this.link = link;
        this.partition = partition;
    }

    public UUID txId() {
        return txId;
    }

    public TxMeta txMeta() {
        return txMeta;
    }

    @Override public void link(long link) {
        this.link = link;
    }

    public long link() {
        return link;
    }

    @Override public int partition() {
        return partition;
    }

    @Override public int size() throws IgniteInternalCheckedException {
        int size = Long.BYTES * 2; // Tx id.

        size += 4; // Tx state.

        size += Long.BYTES; // Commit timestamp.

        size += txMeta.enlistedPartitions().size() * Integer.BYTES * 2; // Enlisted partitions.

        return size;
    }

    @Override public int headerSize() {
        return 4;
    }

    @Override public IoVersions<? extends AbstractDataPageIo> ioVersions() {
        return TxMetaStorageDataIo.VERSIONS;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        TxMetaRowWrapper wrapper = (TxMetaRowWrapper) o;

        return Objects.equals(txId, wrapper.txId);
    }

    @Override public int hashCode() {
        return Objects.hash(txId);
    }

    @Override public int compareTo(@NotNull TxMetaRowWrapper w) {
        if (this == w)
            return 0;

        return txId.compareTo(w.txId);
    }

    public static IgniteBiTuple<UUID, TxMeta> unwrap(TxMetaRowWrapper wrapper) {
        return wrapper == null ? new IgniteBiTuple<>(null, null) : new IgniteBiTuple<>(wrapper.txId, wrapper.txMeta);
    }
}
