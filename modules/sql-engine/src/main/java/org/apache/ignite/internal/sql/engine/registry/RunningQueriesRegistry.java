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

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapper;
import org.apache.ignite.internal.sql.engine.tx.ScriptTransactionContext;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.jetbrains.annotations.Nullable;

/**
 * Registry that keeps track of running queries and open cursors.
 */
public interface RunningQueriesRegistry {
    /** Returns number of opened cursors. */
    int openedCursorsCount();

    /** Returns list of running queries. */
    Collection<RunningQueryInfo> queries();

    /** Registers a new query. */
    RunningQueryInfo registerQuery(String schema, String sql, @Nullable QueryTransactionWrapper txWrapper);

    /** Registers a new multi-statement query. */
    RunningScriptInfoTracker registerScript(String schema, String sql, ScriptTransactionContext scriptTxContext);

    /** Associates a new cursor with the query. */
    void registerCursor(RunningQueryInfo queryInfo, AsyncSqlCursor<?> cursor);

    /** Unregisters the query. */
    void unregister(UUID uuid);

    /** Returns a system view for running queries. */
    SystemView<RunningQueryInfo> asSystemView();
}
