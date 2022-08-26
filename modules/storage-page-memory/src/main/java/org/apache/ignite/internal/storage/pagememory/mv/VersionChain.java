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

    private final @Nullable UUID transactionId;

    /** Link to the most recent version. */
    private final long headLink;

    /** Link to the newest committed {@link RowVersion} if head is not yet committed, or {@link RowVersion#NULL_LINK} otherwise. */
    private final long nextLink;

    /**
     * Constructor.
     */
    public VersionChain(RowId rowId, @Nullable UUID transactionId, long headLink, long nextLink) {
        super(rowId);
        this.transactionId = transactionId;
        this.headLink = headLink;
        this.nextLink = nextLink;
    }

    /**
     * Returns a transaction id, associated with a chain's head, or {@code null} if head is already committed.
     */
    public @Nullable UUID transactionId() {
        return transactionId;
    }

    /**
     * Returns a link to the newest {@link RowVersion} in the chain.
     */
    public long headLink() {
        return headLink;
    }

    /**
     * Returns a link to the newest committed {@link RowVersion} if head is not yet committed, or {@link RowVersion#NULL_LINK} otherwise.
     *
     * @see #isUncommitted()
     * @see #newestCommittedLink()
     */
    public long nextLink() {
        return nextLink;
    }

    /**
     * Returns a link to the newest committed {@link RowVersion}. Matches {@link #headLink()} for committed head. Matches
     * {@link #nextLink()} otherwise.
     */
    public long newestCommittedLink() {
        return isUncommitted() ? nextLink : headLink;
    }

    /**
     * Returns {@code true} if chain's head is uncommitted, by comparing {@link #transactionId()} with {@code null}.
     */
    public boolean isUncommitted() {
        return transactionId != null;
    }

    /**
     * Returns {@code true} if this version chain has at least one committed version.
     */
    public boolean hasCommittedVersions() {
        return newestCommittedLink() != RowVersion.NULL_LINK;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(VersionChain.class, this);
    }
}
