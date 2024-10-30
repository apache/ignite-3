package org.apache.ignite.internal.sql.engine.exec.fsm;

import java.util.List;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.SqlException;

class Programs {
    static final Program<AsyncSqlCursor<InternalSqlRow>> QUERY_EXECUTION = new Program<>(
            "QUERY_EXECUTION",
            List.of(
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
            ),
            phase -> phase == ExecutionPhase.EXECUTING,
            query -> query.cursor
    );

    static final Program<AsyncSqlCursor<InternalSqlRow>> SCRIPT_ITEM_EXECUTION = new Program<>(
            "SCRIPT_ITEM_EXECUTION",
            List.of(
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
                            query -> ExecutionPhase.EXECUTING
                    )
            ),
            phase -> phase == ExecutionPhase.EXECUTING,
            query -> query.cursor
    );

    static final Program<AsyncSqlCursor<InternalSqlRow>> QUERY_OPTIMIZATION = new Program<>(
            "QUERY_OPTIMIZATION",
            List.of(
                    new Transition(
                            ExecutionPhase.REGISTERED,
                            query -> ExecutionPhase.PARSING
                    ),
                    new Transition(
                            ExecutionPhase.PARSING,
                            query -> {
                                if (query.parsedResult == null) {
                                    throw new SqlException(Sql.STMT_VALIDATION_ERR);
                                }

                                return ExecutionPhase.OPTIMIZING;
                            })
            ),
            phase -> phase == ExecutionPhase.OPTIMIZING,
            query -> query.cursor
    );
}
