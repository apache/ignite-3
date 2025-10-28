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

import static org.apache.ignite.internal.sql.engine.util.Commons.isMultiStatementQueryAllowed;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;

/** Parses the query string and populate {@link Query query state} with results. */
class ParsingPhaseHandler implements ExecutionPhaseHandler {
    static final ExecutionPhaseHandler INSTANCE = new ParsingPhaseHandler();

    private ParsingPhaseHandler() { }

    @Override
    public Result handle(Query query) {
        ParsedResult parsedResult = query.executor.lookupParsedResultInCache(query.sql);

        if (parsedResult != null) {
            query.parsedResult = parsedResult;

            return Result.completed();
        }

        CompletableFuture<Void> awaitFuture = new CompletableFuture<>();
        query.executor.execute(() -> {
            try {
                ParsedResult result;
                if (isMultiStatementQueryAllowed(query.properties)) {
                    List<ParsedResult> results = query.executor.parseScript(query.sql);

                    if (results.size() != 1 || results.get(0).queryType() == SqlQueryType.TX_CONTROL) {
                        // the query is script indeed, need different execution path
                        query.parsedScript = results;

                        awaitFuture.complete(null);

                        return;
                    }

                    result = results.get(0);
                } else {
                    result = query.executor.parse(query.sql);
                }

                if (result.queryType().supportsParseResultCaching()) {
                    query.executor.updateParsedResultCache(query.sql, result);
                }

                query.parsedResult = result;

                awaitFuture.complete(null);
            } catch (Throwable th) {
                awaitFuture.completeExceptionally(th);
            }
        });

        return Result.proceedAfter(awaitFuture);
    }
}
