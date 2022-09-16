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

package org.apache.ignite.internal.metastorage.common.command;

import static java.util.Objects.requireNonNull;

import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.raft.client.WriteCommand;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Range command for MetaStorageCommandListener that retrieves entries for the given key range in lexicographic order. Entries will be
 * filtered out by upper bound of given revision number.
 */
public final class RangeCommand implements WriteCommand {
    /** Default value for {@link #batchSize}. */
    public static final int DEFAULT_BATCH_SIZE = 100;

    /** Start key of range (inclusive). Couldn't be {@code null}. */
    @NotNull
    private final byte[] keyFrom;

    /** End key of range (exclusive). Could be {@code null}. */
    @Nullable
    private final byte[] keyTo;

    /** The upper bound for entry revision. {@code -1} means latest revision. */
    @NotNull
    private final long revUpperBound;

    /** Id of the node that requests range. */
    @NotNull
    private final String requesterNodeId;

    /** Id of cursor that is associated with the current command. */
    @NotNull
    private final IgniteUuid cursorId;

    /** Whether to include tombstone entries. */
    private final boolean includeTombstones;

    /** Maximum size of the batch that is sent in single response message. */
    private final int batchSize;

    /**
     * Constructor.
     *
     * @param keyFrom           Start key of range (inclusive).
     * @param keyTo             End key of range (exclusive).
     * @param revUpperBound     The upper bound for entry revision. {@code -1} means latest revision.
     * @param requesterNodeId   Id of the node that requests range.
     * @param cursorId          Id of cursor that is associated with the current command.
     * @param includeTombstones Whether to include tombstones.
     * @param batchSize         Maximum size of the batch that is sent in single response message.
     */
    public RangeCommand(
            @NotNull ByteArray keyFrom,
            @Nullable ByteArray keyTo,
            long revUpperBound,
            @NotNull String requesterNodeId,
            @NotNull IgniteUuid cursorId,
            boolean includeTombstones,
            int batchSize
    ) {
        this.keyFrom = keyFrom.bytes();
        this.keyTo = keyTo == null ? null : keyTo.bytes();
        this.revUpperBound = revUpperBound;
        this.requesterNodeId = requesterNodeId;
        this.cursorId = cursorId;
        this.includeTombstones = includeTombstones;
        this.batchSize = batchSize;
    }

    /**
     * Returns start key of range (inclusive). Couldn't be {@code null}.
     */
    public @NotNull byte[] keyFrom() {
        return keyFrom;
    }

    /**
     * Returns end key of range (exclusive). Could be {@code null}.
     */
    public @Nullable byte[] keyTo() {
        return keyTo;
    }

    /**
     * Returns the upper bound for entry revision. Means latest revision.
     */
    public @NotNull long revUpperBound() {
        return revUpperBound;
    }

    /**
     * Returns id of the node that requests range.
     */
    public @NotNull String requesterNodeId() {
        return requesterNodeId;
    }

    /**
     * Returns id of cursor that is associated with the current command.
     */
    @NotNull
    public IgniteUuid getCursorId() {
        return cursorId;
    }

    /**
     * Returns the boolean value indicating whether this range command is supposed to include tombstone entries into the cursor.
     */
    public boolean includeTombstones() {
        return includeTombstones;
    }

    public int batchSize() {
        return batchSize;
    }

    public static RangeCommandBuilder builder(@NotNull ByteArray keyFrom, @NotNull String requesterNodeId, @NotNull IgniteUuid cursorId) {
        return new RangeCommandBuilder(keyFrom, requesterNodeId, cursorId);
    }

    /**
     * The builder.
     */
    public static class RangeCommandBuilder {
        private final ByteArray keyFrom;

        private ByteArray keyTo;

        private long revUpperBound = -1L;

        private final String requesterNodeId;

        private final IgniteUuid cursorId;

        private boolean includeTombstones = false;

        private int batchSize = DEFAULT_BATCH_SIZE;

        /**
         * The builder constructor.
         */
        public RangeCommandBuilder(@NotNull ByteArray keyFrom, @NotNull String requesterNodeId, @NotNull IgniteUuid cursorId) {
            this.keyFrom = keyFrom;
            this.requesterNodeId = requesterNodeId;
            this.cursorId = cursorId;
        }

        /**
         * Setter for key to.
         *
         * @param keyTo Key to.
         * @return This for chaining.
         */
        public RangeCommandBuilder keyTo(ByteArray keyTo) {
            this.keyTo = keyTo;

            return this;
        }

        /**
         * Setter for upper bound revision.
         *
         * @param revUpperBound Upper bound revision.
         * @return This for chaining.
         */
        public RangeCommandBuilder revUpperBound(long revUpperBound) {
            this.revUpperBound = revUpperBound;

            return this;
        }

        /**
         * Setter for include tombstones.
         *
         * @param includeTombstones Whether to include tombstones.
         * @return This for chaining.
         */
        public RangeCommandBuilder includeTombstones(boolean includeTombstones) {
            this.includeTombstones = includeTombstones;

            return this;
        }

        /**
         * Setter for batch size.
         *
         * @param batchSize Batch size.
         * @return This for chaining.
         */
        public RangeCommandBuilder batchSize(int batchSize) {
            this.batchSize = batchSize;

            return this;
        }

        /**
         * Build method.
         *
         * @return Range command.
         */
        public RangeCommand build() {
            return new RangeCommand(
                requireNonNull(keyFrom),
                keyTo,
                revUpperBound,
                requireNonNull(requesterNodeId),
                requireNonNull(cursorId),
                includeTombstones,
                batchSize
            );
        }
    }
}
