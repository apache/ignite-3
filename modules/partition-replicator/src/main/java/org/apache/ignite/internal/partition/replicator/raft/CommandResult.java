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

package org.apache.ignite.internal.partition.replicator.raft;

import java.io.Serializable;
import org.jetbrains.annotations.Nullable;

/**
 * Result of a Raft command processing.
 */
public class CommandResult {
    public static final CommandResult EMPTY_APPLIED_RESULT = new CommandResult(null, true);

    public static final CommandResult EMPTY_NOT_APPLIED_RESULT = new CommandResult(null, false);

    @Nullable
    private final Serializable result;

    private final boolean wasApplied;

    public CommandResult(@Nullable Serializable result, boolean wasApplied) {
        this.result = result;
        this.wasApplied = wasApplied;
    }

    /**
     * Returns the result of the command processing.
     */
    public @Nullable Serializable result() {
        return result;
    }

    /**
     * Returns {@code true} if the command was actually applied to the state machine.
     */
    public boolean wasApplied() {
        return wasApplied;
    }
}
