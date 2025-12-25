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

import static org.apache.ignite.internal.sql.engine.exec.fsm.ValidationHelper.validateDynamicParameters;
import static org.apache.ignite.internal.sql.engine.exec.fsm.ValidationHelper.validateParsedStatement;
import static org.apache.ignite.lang.ErrorGroups.Sql.RUNTIME_ERR;

import java.time.ZoneId;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.sql.engine.SqlOperationContext;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.prepare.PrepareService;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionContext;
import org.apache.ignite.internal.sql.engine.tx.QueryTransactionWrapper;
import org.apache.ignite.sql.SqlException;

/** Validates parsed AST acquired on the previous phase and submit optimization task to {@link PrepareService}. */
class OptimizingPhaseHandler implements ExecutionPhaseHandler {
    static final ExecutionPhaseHandler INSTANCE = new OptimizingPhaseHandler();

    private OptimizingPhaseHandler() {
    }

    @Override
    public Result handle(Query query) {
        ParsedResult result = query.parsedResult;

        assert result != null : "Query is expected to be parsed at this phase";

        SqlOperationContext retryOperationContext = query.operationContext;
        SqlOperationContext operationContext = retryOperationContext == null
                ? buildContext(query, result)
                : retryOperationContext.withOperationTime(() -> query.executor.deriveOperationTime(query.txContext));

        query.operationContext = operationContext;

        CompletableFuture<Void> awaitFuture = query.executor.waitForMetadata(operationContext.operationTime())
                .thenCompose(none -> query.executor.prepare(result, operationContext)
                        .thenAccept(plan -> {
                            if (query.txContext.explicitTx() == null) {
                                // in case of implicit tx we have to update observable time to prevent tx manager to start
                                // implicit transaction too much in the past where version of catalog we used to prepare the
                                // plan was not yet available
                                query.txContext.updateObservableTime(query.executor.deriveMinimalRequiredTime(plan));
                            }

                            query.plan = plan;
                        }));

        return Result.proceedAfter(awaitFuture);
    }

    private static SqlOperationContext buildContext(Query query, ParsedResult result) {
        validateParsedStatement(query.properties, result);
        validateDynamicParameters(result.dynamicParamsCount(), query.params, true);
        ensureStatementMatchesTx(result.queryType(), query.txContext);

        HybridTimestamp operationTime = query.executor.deriveOperationTime(query.txContext);

        String schemaName = query.properties.defaultSchema();
        ZoneId timeZoneId = query.properties.timeZoneId();
        String userName = query.properties.userName();

        return  SqlOperationContext.builder()
                .queryId(query.id)
                .cancel(query.cancel)
                .parameters(query.params)
                .timeZoneId(timeZoneId)
                .defaultSchemaName(schemaName)
                .operationTime(operationTime)
                .txContext(query.txContext)
                .txUsedListener(tx -> query.usedTransaction = tx)
                .errorHandler(query::setError)
                .userName(userName)
                .build();
    }

    /** Checks that the statement is allowed within an external/script transaction. */
    private static void ensureStatementMatchesTx(SqlQueryType queryType, QueryTransactionContext txContext) {
        QueryTransactionWrapper txWrapper = txContext.explicitTx();

        if (txWrapper == null) {
            return;
        }

        if (!queryType.supportsExplicitTransactions()) {
            throw new SqlException(RUNTIME_ERR, queryType.displayName() + " doesn't support transactions.");
        }

        if (SqlQueryType.DML == queryType && txWrapper.unwrap().isReadOnly()) {
            throw new SqlException(RUNTIME_ERR, queryType.displayName() + " cannot be started by using read only transactions.");
        }
    }
}
