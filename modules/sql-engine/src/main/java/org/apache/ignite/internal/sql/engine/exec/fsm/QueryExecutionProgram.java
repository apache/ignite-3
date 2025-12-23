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

import static org.apache.ignite.lang.ErrorGroups.Replicator;
import static org.apache.ignite.lang.ErrorGroups.Transactions;

import java.util.List;
import org.apache.ignite.internal.partition.replicator.schemacompat.InternalSchemaVersionMismatchException;
import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.apache.ignite.internal.sql.engine.SqlOperationContext;
import org.apache.ignite.internal.sql.engine.exec.ExecutablePlan;
import org.apache.ignite.internal.sql.engine.exec.MultiStepPlanOutdatedException;
import org.apache.ignite.internal.sql.engine.message.UnknownNodeException;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapper;
import org.apache.ignite.internal.util.ExceptionUtils;

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
                    query -> ExecutionPhase.CURSOR_PUBLICATION
            ),
            new Transition(
                    ExecutionPhase.CURSOR_PUBLICATION,
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
                QueryExecutionProgram::errorHandler
        );
    }

    static boolean errorHandler(Query query, Throwable th) {
        if (canRecover(query, th)) {
            query.error.set(null);

            if (multiStepPlanOutdated(th)) {
                assert query.plan instanceof MultiStepPlan;
                assert query.currentPhase() == ExecutionPhase.CURSOR_INITIALIZATION : query.currentPhase();

                SqlOperationContext context = query.operationContext;
                QueryTransactionWrapper txWrapper = query.usedTransaction;

                assert context != null;
                assert txWrapper != null;

                context.usedTx(txWrapper);

                query.moveTo(ExecutionPhase.OPTIMIZING);

                return true;
            }

            if (fastPlanSchemaMismatch(th)) {
                assert query.plan instanceof ExecutablePlan;

                query.moveTo(ExecutionPhase.OPTIMIZING);

                return true;
            }

            if (query.currentPhase() == ExecutionPhase.CURSOR_PUBLICATION) {
                // Should initialize a new cursor.
                query.moveTo(ExecutionPhase.CURSOR_INITIALIZATION);
            }

            if (nodeLeft(th)) {
                SqlOperationContext context = query.operationContext;

                assert context != null;

                // ensured by nodeLeft() method
                assert th instanceof UnknownNodeException : th;

                UnknownNodeException exception = (UnknownNodeException) th;

                context.excludeNode(exception.nodeName());

                return true;
            }

            return lockConflict(th) || replicaMiss(th) || groupOverloaded(th) || tableSchemaChanged(th);
        }

        return false;
    }

    @SuppressWarnings("SimplifiableIfStatement")
    private static boolean canRecover(Query query, Throwable th) {
        if (query.currentPhase() != ExecutionPhase.CURSOR_INITIALIZATION
                && query.currentPhase() != ExecutionPhase.CURSOR_PUBLICATION) {
            return false;
        }

        // DataCursor unconditionally rolls back transaction in case of error, therefore it's not possible
        // to recover any explicit transaction at the moment
        if (query.txContext.explicitTx() != null) {
            return false;
        }

        return nodeLeft(th) || lockConflict(th) || replicaMiss(th) || groupOverloaded(th)
                || multiStepPlanOutdated(th) || tableSchemaChanged(th) || fastPlanSchemaMismatch(th);
    }

    private static boolean nodeLeft(Throwable th) {
        return th instanceof UnknownNodeException;
    }

    private static boolean lockConflict(Throwable th) {
        return ExceptionUtils.extractCodeFrom(th) == Transactions.ACQUIRE_LOCK_ERR;
    }

    private static boolean replicaMiss(Throwable th) {
        return ExceptionUtils.extractCodeFrom(th) == Replicator.REPLICA_MISS_ERR;
    }

    private static boolean groupOverloaded(Throwable th) {
        return ExceptionUtils.extractCodeFrom(th) == Replicator.GROUP_OVERLOADED_ERR;
    }

    private static boolean multiStepPlanOutdated(Throwable th)  {
        return th instanceof MultiStepPlanOutdatedException;
    }

    private static boolean tableSchemaChanged(Throwable th) {
        return ExceptionUtils.extractCodeFrom(th) == Transactions.TX_INCOMPATIBLE_SCHEMA_ERR;
    }

    private static boolean fastPlanSchemaMismatch(Throwable th) {
        return ExceptionUtils.hasCause(th, InternalSchemaVersionMismatchException.class);
    }
}
