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

import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;

/** Enumerates all programs available for execution. */
class Programs {
    /** General execution program. Accepts query string as input and returns cursor as result of execution. */
    static final Program<AsyncSqlCursor<InternalSqlRow>> QUERY_EXECUTION = QueryExecutionProgram.INSTANCE;

    /** A program to execute child query within a script. As input expects already parsed AST and returns cursor as result. */
    static final Program<AsyncSqlCursor<InternalSqlRow>> SCRIPT_ITEM_EXECUTION = ScriptItemExecutionProgram.INSTANCE;

    /** A program to prepare child query within a script. As input expects already parsed AST and returns prepared plan as result. */
    static final Program<QueryPlan> SCRIPT_ITEM_PREPARATION = ScriptItemPrepareProgram.INSTANCE;
}
