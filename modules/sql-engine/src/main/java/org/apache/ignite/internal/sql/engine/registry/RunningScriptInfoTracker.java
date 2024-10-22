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

package org.apache.ignite.internal.sql.engine.registry;

import java.util.UUID;
import org.apache.ignite.internal.sql.engine.SqlQueryType;

/**
 * Manages information about the execution of a multi-statement query.
 *
 * <ul>
 * <li>The script has two phases: {@link QueryExecutionPhase#INITIALIZATION initialization}
 *      and {@link QueryExecutionPhase#EXECUTION execution}.</li>
 * <li>The script unregisters itself in the {@link RunningQueriesRegistry} when the last
 *     sub-statement unregisters itself.</li>
 * </ul>
 */
public interface RunningScriptInfoTracker {
    /** Returns the identifier of multi-statement query. */
    UUID queryId();

    /** Registers a new statement in the {@link RunningQueriesRegistry}. */
    RunningQueryInfo registerStatement(String sql, SqlQueryType queryType);

    /** Must be called when sub-statement unregisters from the {@link RunningQueriesRegistry}. */
    void onStatementUnregistered();

    /** Sets remaining sub-statements count and switches script execution phase to {@link QueryExecutionPhase#EXECUTION}. */
    void onInitializationComplete(int statementsCount);
}
