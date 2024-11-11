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
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.engine.sql.ParsedResult;

/** Initializes script handler and kick starts script execution. */
class ScriptInitializationPhaseHandler implements ExecutionPhaseHandler {
    static final ExecutionPhaseHandler INSTANCE = new ScriptInitializationPhaseHandler();

    private ScriptInitializationPhaseHandler() {
    }

    @Override
    public Result handle(Query query) {
        List<ParsedResult> parsedResults = query.parsedScript;

        assert parsedResults != null;

        CompletableFuture<Void> awaitFuture = query.executor.createScriptHandler(query)
                .processNext()
                .thenAccept(cursor -> {
                    query.cursor = cursor;

                    query.moveTo(ExecutionPhase.EXECUTING);
                });

        return Result.proceedAfter(awaitFuture);
    }
}
