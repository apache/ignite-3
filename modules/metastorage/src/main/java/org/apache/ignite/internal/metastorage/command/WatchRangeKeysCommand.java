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
import org.jetbrains.annotations.Nullable;

/**
 * Watch command for MetaStorageCommandListener that subscribes on meta storage updates matching the parameters.
 */
@Transferable(MetastorageCommandsMessageGroup.WATCH_RANGE_KEYS)
public interface WatchRangeKeysCommand extends WriteCommand {
    /**
     * Returns start key of range (inclusive). Couldn't be {@code null}.
     */
    byte @Nullable [] keyFrom();

    /**
     * Returns end key of range (exclusive). Could be {@code null}.
     */
    byte @Nullable [] keyTo();

    /**
     * Returns start revision inclusive. {@code 0} - all revisions.
     */
    long revision();

    /**
     * Returns id of the node that requests range.
     */
    String requesterNodeId();

    /**
     * Returns id of cursor that is associated with the current command.
     */
    IgniteUuid cursorId();
}
