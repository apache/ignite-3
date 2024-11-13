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

import java.util.List;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;

/**
 * Generic execution program, which accepts query string and initialization parameters as input, and returns cursor as result.
 */
class QueryExecutionProgram extends Program<AsyncSqlCursor<InternalSqlRow>> {
    private static final String PROGRAM_NAME = "QUERY_EXECUTION";
    private static final List<Transition> TRANSITIONS = List.of(
            new Transition(
                    ExecutionPhase.REGISTERED,
                    query -> ExecutionPhase.PARSING
            ),
            new Transition(
                    ExecutionPhase.PARSING,
                    query -> query.parsedResult != null
                            ? ExecutionPhase.OPTIMIZING
                            : ExecutionPhase.SCRIPT_INITIALIZATION),
            new Transition(
                    ExecutionPhase.OPTIMIZING,
                    query -> ExecutionPhase.CURSOR_INITIALIZATION
            ),
            new Transition(
                    ExecutionPhase.CURSOR_INITIALIZATION,
                    query -> ExecutionPhase.EXECUTING
            ),
            new Transition(
                    ExecutionPhase.SCRIPT_INITIALIZATION,
                    query -> ExecutionPhase.EXECUTING
            )
    );

    static final Program<AsyncSqlCursor<InternalSqlRow>> INSTANCE = new QueryExecutionProgram();

    private QueryExecutionProgram() {
        super(
                PROGRAM_NAME,
                TRANSITIONS,
                phase -> phase == ExecutionPhase.EXECUTING,
                query -> query.cursor,
                (query, throwable) -> {
                    query.onError(throwable);

                    return false;
                }
        );
    }
}
