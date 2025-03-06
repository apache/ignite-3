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

package org.apache.ignite.internal.partition.replicator.raft.handlers;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.partition.replicator.raft.CommandResult;
import org.apache.ignite.internal.raft.WriteCommand;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for raft command handlers.
 */
public abstract class AbstractCommandHandler<T extends WriteCommand> {
    /**
     * Handles a command.
     *
     * @param command Write Command.
     * @param commandIndex Index of the RAFT command.
     * @param commandTerm Term of the RAFT command.
     * @param safeTimestamp Safe timestamp.
     * @throws IgniteInternalException if an exception occurred during processing the command.
     */
    public final CommandResult handle(
            WriteCommand command,
            long commandIndex,
            long commandTerm,
            @Nullable HybridTimestamp safeTimestamp
    ) throws IgniteInternalException {
        return handleInternally((T) command, commandIndex, commandTerm, safeTimestamp);
    }

    /**
     * Handles a command.
     *
     * @param command Write Command.
     * @param commandIndex Index of the RAFT command.
     * @param commandTerm Term of the RAFT command.
     * @param safeTimestamp Safe timestamp.
     */
    protected abstract CommandResult handleInternally(
            T command,
            long commandIndex,
            long commandTerm,
            @Nullable HybridTimestamp safeTimestamp) throws IgniteInternalException;
}
