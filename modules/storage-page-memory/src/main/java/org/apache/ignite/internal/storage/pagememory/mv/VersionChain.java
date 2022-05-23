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

import java.util.UUID;
import org.apache.ignite.internal.pagememory.Storable;
import org.apache.ignite.internal.pagememory.io.AbstractDataPageIo;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.storage.pagememory.mv.io.VersionChainDataIo;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Represents row version chain: that is, all versions of the row plus some row-level metadata.
 *
 * <p>NB: this represents the whole set of versions, not just one version in the chain.
 */
public class VersionChain extends VersionChainLink implements Storable {
    public static long NULL_UUID_COMPONENT = 0;

    private static final int TRANSACTION_ID_STORE_SIZE_BYTES = 2 * Long.BYTES;
    private static final int HEAD_LINK_STORE_SIZE_BYTES = PartitionlessLinks.PARTITIONLESS_LINK_SIZE_BYTES;

    public static final int TRANSACTION_ID_OFFSET = 0;

    private final int partitionId;
    @Nullable
    private final UUID transactionId;
    private final long headLink;

    // TODO: IGNITE-17008 - add nextLink

    /**
     * Constructs a VersionChain without a transaction ID.
     */
    public static VersionChain withoutTxId(int partitionId, long link, long headLink) {
        return new VersionChain(partitionId, link, null, headLink);
    }

    /**
     * Constructor.
     */
    public VersionChain(int partitionId, @Nullable UUID transactionId, long headLink) {
        this.partitionId = partitionId;
        this.transactionId = transactionId;
        this.headLink = headLink;
    }

    /**
     * Constructor.
     */
    public VersionChain(int partitionId, long link, @Nullable UUID transactionId, long headLink) {
        super(link);
        this.partitionId = partitionId;
        this.transactionId = transactionId;
        this.headLink = headLink;
    }

    @Nullable
    public UUID transactionId() {
        return transactionId;
    }

    public long headLink() {
        return headLink;
    }

    @Override
    public final int partition() {
        return partitionId;
    }

    @Override
    public int size() {
        return TRANSACTION_ID_STORE_SIZE_BYTES + HEAD_LINK_STORE_SIZE_BYTES;
    }

    @Override
    public int headerSize() {
        return size();
    }

    @Override
    public IoVersions<? extends AbstractDataPageIo> ioVersions() {
        return VersionChainDataIo.VERSIONS;
    }

    @Override
    public String toString() {
        return S.toString(VersionChain.class, this);
    }
}
