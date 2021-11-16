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

package org.apache.ignite.internal.metastorage.common.command;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.raft.client.WriteCommand;
import org.jetbrains.annotations.NotNull;

/**
 * Watch command for MetaStorageCommandListener that subscribes on meta storage updates matching the parameters.
 */
public final class WatchExactKeysCommand implements WriteCommand {
    /** The keys list. Couldn't be {@code null}. */
    @NotNull
    private final List<byte[]> keys;

    /** Start revision inclusive. {@code 0} - all revisions. */
    private final long revision;

    /** Id of the node that requests watch. */
    @NotNull
    private final String requesterNodeId;

    /** Id of cursor that is associated with the current command. */
    @NotNull
    private final IgniteUuid cursorId;

    /**
     * Constructor.
     *
     * @param keys            The keys collection. Couldn't be {@code null}.
     * @param revision        Start revision inclusive. {@code 0} - all revisions.
     * @param requesterNodeId Id of the node that requests watch.
     * @param cursorId        Id of cursor that is associated with the current command.
     */
    public WatchExactKeysCommand(
            @NotNull Set<ByteArray> keys,
            long revision,
            @NotNull String requesterNodeId,
            @NotNull IgniteUuid cursorId
    ) {
        this.keys = new ArrayList<>(keys.size());

        for (ByteArray key : keys) {
            this.keys.add(key.bytes());
        }

        this.revision = revision;

        this.requesterNodeId = requesterNodeId;

        this.cursorId = cursorId;
    }

    /**
     * Returns the keys list. Couldn't be {@code null}.
     */
    public @NotNull List<byte[]> keys() {
        return keys;
    }

    /**
     * Returns start revision inclusive.
     */
    public @NotNull Long revision() {
        return revision;
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
}
