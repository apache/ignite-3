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
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Represents row version chain: that is, all versions of the row plus some row-level metadata.
 *
 * <p>NB: this represents the whole set of versions, not just one version in the chain.
 */
public class VersionChain extends VersionChainKey {
    public static final long NULL_UUID_COMPONENT = 0;

    @Nullable
    private final UUID transactionId;

    /**
     * Link to the latest version.
     */
    private final long headLink;

    /**
     * Link to the pre-latest version ({@link RowVersion#isNullLink(long)} is {@code true} if there is just one version).
     */
    private final long nextLink;

    /**
     * Constructs a VersionChain without a transaction ID.
     */
    public static VersionChain withoutTxId(RowId rowId, long headLink, long nextLink) {
        return new VersionChain(rowId, null, headLink, nextLink);
    }

    /**
     * Constructor.
     */
    public VersionChain(RowId rowId, @Nullable UUID transactionId, long headLink, long nextLink) {
        super(rowId);
        this.transactionId = transactionId;
        this.headLink = headLink;
        this.nextLink = nextLink;
    }

    @Nullable
    public UUID transactionId() {
        return transactionId;
    }

    public long headLink() {
        return headLink;
    }

    public long nextLink() {
        return nextLink;
    }

    public long newestCommittedLink() {
        return isUncommitted() ? nextLink : headLink;
    }

    public boolean isUncommitted() {
        return transactionId != null;
    }

    public boolean hasCommittedVersions() {
        return !RowVersion.isNullLink(newestCommittedLink());
    }

    @Override
    public String toString() {
        return S.toString(VersionChain.class, this);
    }
}
