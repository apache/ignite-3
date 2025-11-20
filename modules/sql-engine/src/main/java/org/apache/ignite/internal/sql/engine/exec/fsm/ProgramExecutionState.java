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

import static org.apache.ignite.internal.lang.SqlExceptionMapperUtil.mapToPublicSqlException;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.tostring.S;

/**
 * Represents a state of program execution.
 *
 * <p>That is, strictly typed holder of an execution result.
 *
 * @param <ResultT> A type of the execution result.
 */
class ProgramExecutionState<ResultT> implements ProgramExecutionHandle {
    final CompletableFuture<Void> programFinished = new CompletableFuture<>();
    final CompletableFuture<ResultT> resultHolder = new CompletableFuture<>();

    @SuppressWarnings("FieldCanBeLocal") // Used in toString()
    private final String programName;

    ProgramExecutionState(String programName) {
        this.programName = programName;
    }

    @Override
    public void notifyError(Throwable th) {
        resultHolder.completeExceptionally(mapToPublicSqlException(unwrapCause(th)));
    }

    @Override
    public CompletableFuture<Void> completionFuture() {
        return programFinished;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
