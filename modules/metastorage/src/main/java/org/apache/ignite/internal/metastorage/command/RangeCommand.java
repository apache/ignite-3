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

package org.apache.ignite.internal.metastorage.command;

import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.annotations.Transferable;

/**
 * Range command for MetaStorageCommandListener that retrieves entries for the given key range in lexicographic order. Entries will be
 * filtered out by upper bound of given revision number.
 */
@Transferable(MetastorageCommandsMessageGroup.RANGE)
public interface RangeCommand extends WriteCommand {
    /** Default value for {@link #batchSize}. */
    int DEFAULT_BATCH_SIZE = 100;

    /**
     * Returns start key of range (inclusive). Couldn't be {@code null}.
     */
    byte[] keyFrom();

    /**
     * Returns end key of range (exclusive). Could be {@code null}.
     */
    byte[] keyTo();

    /**
     * Returns the upper bound for entry revision. Means latest revision.
     */
    long revUpperBound();

    /**
     * Returns id of the node that requests range.
     */
    String requesterNodeId();

    /**
     * Returns id of cursor that is associated with the current command.
     */
    IgniteUuid cursorId();

    /**
     * Returns the boolean value indicating whether this range command is supposed to include tombstone entries into the cursor.
     */
    boolean includeTombstones();

    /**
     * Returns maximum size of the batch that is sent in single response message.
     */
    int batchSize();
}
