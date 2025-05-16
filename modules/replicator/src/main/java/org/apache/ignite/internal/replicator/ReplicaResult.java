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

package org.apache.ignite.internal.replicator;

import java.util.function.BiConsumer;
import org.jetbrains.annotations.Nullable;

/**
 * Represents replica execution result.
 */
public class ReplicaResult {
    /** Default replication outcome result. */
    private static final CommandApplicationResult DEFAULT_RESULT = new CommandApplicationResult(null, null);

    /** The result. */
    private final Object res;

    /** The replication future. */
    private final CommandApplicationResult commandApplicationResult;

    /** Delayed ack processor. */
    public @Nullable BiConsumer<Object, Throwable> delayedAckProcessor;

    /**
     * Construct a replica result.
     *
     * @param res The result.
     * @param commandApplicationResult The replication result.
     */
    public ReplicaResult(@Nullable Object res, @Nullable CommandApplicationResult commandApplicationResult) {
        this.res = res;
        this.commandApplicationResult = commandApplicationResult == null ? DEFAULT_RESULT : commandApplicationResult;
    }

    /**
     * Get the result.
     *
     * @return The result.
     */
    public @Nullable Object result() {
        return res;
    }

    /**
     * Get the replication future.
     *
     * @return The replication future.
     */
    public CommandApplicationResult applyResult() {
        return commandApplicationResult;
    }
}
