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

package org.apache.ignite.internal.metastorage.command.cursor;

import org.apache.ignite.internal.metastorage.command.MetastorageCommandsMessageGroup;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.annotations.Transferable;

/**
 * Raft command for getting all Meta Storage keys that start with a given prefix.
 */
@Transferable(MetastorageCommandsMessageGroup.CREATE_PREFIX_CURSOR)
public interface CreatePrefixCursorCommand extends WriteCommand {
    /**
     * Returns key prefix.
     */
    byte[] prefix();

    /**
     * Returns the upper bound for entry revision.
     */
    long revUpperBound();

    /**
     * Returns the originating node ID.
     */
    String requesterNodeId();

    /**
     * Returns id of cursor that is associated with the current command.
     */
    IgniteUuid cursorId();

    /**
     * Returns the boolean value indicating whether this command is supposed to include tombstone entries into the cursor.
     */
    boolean includeTombstones();
}
