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

package org.apache.ignite.internal.sql.engine.exec.fsm;

/**
 * Enumerates possible phases of query execution.
 *
 * <p>This enum is a sort of transition graph since generally there is exactly one transition on a normal execution path. The one exception
 * is {@link #PARSING} phase where path diverges to {@link #OPTIMIZING} and {@link #CURSOR_INITIALIZATION}. Also, it's allowed to move to
 * {@link #TERMINATED} phase from any other phase (usually, in case of exception).
 *
 * <p>Currently, there are following transitions:
 * <pre>
 *     For single statement execution:
 *     REGISTERED --> PARSING --> OPTIMIZING --> CURSOR_INITIALIZATION --> EXECUTING --> TERMINATED
 *
 *     For script execution:
 *     REGISTERED --> PARSING --> SCRIPT_INITIALIZATION --> EXECUTING --> TERMINATED
 *
 *     For single statement execution spawned by script:
 *     REGISTERED --> OPTIMIZING --> CURSOR_INITIALIZATION --> EXECUTING --> TERMINATED
 * </pre>
 */
public enum ExecutionPhase {
    /** Query is registered on server. */
    REGISTERED(RegisteredPhaseHandler.INSTANCE),
    /** Query string is parsed at the moment. Parsed AST may or may not be available yet. */
    PARSING(ParsingPhaseHandler.INSTANCE),
    /** AST is available now, optimization task is submitted. */
    OPTIMIZING(OptimizingPhaseHandler.INSTANCE),
    /** Query has been validated, plan is ready as well. At this point plan is mapped on cluster and cursor are initialised. */
    CURSOR_INITIALIZATION(CursorInitializationPhaseHandler.INSTANCE),
    /** Multiple queries must be scheduled in a way to not to interfere with each other. */
    SCRIPT_INITIALIZATION(ScriptInitializationPhaseHandler.INSTANCE),
    /**
     * All preparations have been done, distributed cursor is ready. Fetching data for cursor is not managed by fsm, therefore processing is
     * temporary stopped.
     */
    EXECUTING(NoOpHandler.INSTANCE),
    /** Query is terminated (either successfully or due to error), and being deregister soon. This is terminal phase. */
    TERMINATED(NoOpHandler.INSTANCE);

    private final ExecutionPhaseHandler handler;

    ExecutionPhase(ExecutionPhaseHandler handler) {
        this.handler = handler;
    }

    /** Evaluates current phase of query execution. */
    Result evaluate(Query query) {
        return handler.handle(query);
    }
}
