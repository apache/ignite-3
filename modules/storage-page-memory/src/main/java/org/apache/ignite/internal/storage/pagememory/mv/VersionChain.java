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

import static org.apache.ignite.internal.pagememory.util.PageIdUtils.NULL_LINK;

import java.util.UUID;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.internal.storage.ReadResult;
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

    /** Link to the most recent version. */
    private final long headLink;

    /** Link to the newest committed {@link RowVersion} if head is not yet committed, or {@link PageIdUtils#NULL_LINK} otherwise. */
    private final long nextLink;

    /** Transaction id (part of transaction state). */
    private final @Nullable UUID transactionId;

    /** Commit table id (part of transaction state). */
    private final @Nullable Integer commitTableId;

    /** Commit partition id (part of transaction state). */
    private final int commitPartitionId;

    /**
     * Constructor.
     */
    private VersionChain(RowId rowId, @Nullable UUID transactionId, @Nullable Integer commitTableId, int commitPartitionId, long headLink,
            long nextLink) {
        super(rowId);
        this.transactionId = transactionId;
        this.commitTableId = commitTableId;
        this.commitPartitionId = commitPartitionId;
        this.headLink = headLink;
        this.nextLink = nextLink;
    }

    public static VersionChain createCommitted(RowId rowId, long headLink, long nextLink) {
        return new VersionChain(rowId, null, null, ReadResult.UNDEFINED_COMMIT_PARTITION_ID, headLink, nextLink);
    }

    public static VersionChain createUncommitted(RowId rowId, UUID transactionId, int commitTableId, int commitPartitionId, long headLink,
            long nextLink) {
        return new VersionChain(rowId, transactionId, commitTableId, commitPartitionId, headLink, nextLink);
    }

    /**
     * Returns a transaction id, associated with a chain's head, or {@code null} if head is already committed.
     */
    public @Nullable UUID transactionId() {
        return transactionId;
    }

    /**
     * Returns a commit table id, associated with a chain's head, or {@code null} if head is already committed.
     */
    public @Nullable Integer commitTableId() {
        return commitTableId;
    }

    /**
     * Returns a commit partition id, associated with a chain's head, or {@code -1} if head is already committed.
     */
    public int commitPartitionId() {
        return commitPartitionId;
    }

    /**
     * Returns a link to the newest {@link RowVersion} in the chain.
     */
    public long headLink() {
        return headLink;
    }

    /**
     * Returns a link to the newest committed {@link RowVersion} if head is not yet committed, or {@link PageIdUtils#NULL_LINK} otherwise.
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
        return newestCommittedLink() != NULL_LINK;
    }

    /**
     * Returns {@code true} if there is a link to the next version.
     */
    public boolean hasNextLink() {
        return nextLink != NULL_LINK;
    }

    /**
     * Returns {@code true} if there is a link to the head version.
     */
    public boolean hasHeadLink() {
        return headLink != NULL_LINK;
    }

    /**
     * Creates a copy of the version chain with new next link.
     *
     * @param nextLink New next link.
     */
    public VersionChain withNextLink(long nextLink) {
        return new VersionChain(rowId, transactionId, commitTableId, commitPartitionId, headLink, nextLink);
    }

    @Override
    public String toString() {
        return S.toString(VersionChain.class, this, "rowId", rowId);
    }
}
