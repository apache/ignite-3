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
 * A program to execute child query within a script. As input expects already parsed AST and returns cursor as result.
 */
class ScriptItemExecutionProgram extends Program<AsyncSqlCursor<InternalSqlRow>> {
    private static final String PROGRAM_NAME = "SCRIPT_ITEM_EXECUTION";
    private static final List<Transition> TRANSITIONS = List.of(
            new Transition(
                    ExecutionPhase.REGISTERED,
                    query -> ExecutionPhase.OPTIMIZING
            ),
            new Transition(
                    ExecutionPhase.OPTIMIZING,
                    query -> ExecutionPhase.CURSOR_INITIALIZATION
            ),
            new Transition(
                    ExecutionPhase.CURSOR_INITIALIZATION,
                    query -> ExecutionPhase.CURSOR_PUBLICATION
            ),
            new Transition(
                    ExecutionPhase.CURSOR_PUBLICATION,
                    query -> ExecutionPhase.EXECUTING
            )
    );

    static final Program<AsyncSqlCursor<InternalSqlRow>> INSTANCE = new ScriptItemExecutionProgram();

    private ScriptItemExecutionProgram() {
        super(
                PROGRAM_NAME,
                TRANSITIONS,
                phase -> phase == ExecutionPhase.EXECUTING,
                query -> query.cursor,
                QueryExecutionProgram::errorHandler
        );
    }
}
