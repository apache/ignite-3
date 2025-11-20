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

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.exec.fsm.Result.Status;
import org.apache.ignite.internal.util.ExceptionUtils;

/**
 * A definition of a program to get the desired result.
 *
 * <p>The definition includes set of {@link Transition transitions} in particular order to move query from one {@link ExecutionPhase} to
 * another, which leads to population of the query {@link Query state}. When a query reaches the terminal phase, the result is derived
 * from the context and returned as result of program execution.
 *
 * @param <ResultT> Type of the result returned by this program.
 */
class Program<ResultT> {
    private static final IgniteLogger LOG = Loggers.forClass(Program.class);

    private final String name;
    private final Map<ExecutionPhase, Transition> transitions;
    private final Predicate<ExecutionPhase> terminalPhase;
    private final Function<Query, ResultT> result;
    /** If error handler returns {@code true}, then error has been successfully processed, and the query may be executed again. */
    private final BiPredicate<Query, Throwable> errorHandler;

    Program(
            String name,
            List<Transition> transitions,
            Predicate<ExecutionPhase> terminalPhase,
            Function<Query, ResultT> result,
            BiPredicate<Query, Throwable> errorHandler
    ) {
        this.name = name;
        this.transitions = new EnumMap<>(transitions.stream()
                .collect(Collectors.toMap(Transition::from, Function.identity())));
        this.terminalPhase = terminalPhase;
        this.result = result;
        this.errorHandler = errorHandler;
    }

    ProgramExecutionState<ResultT> createState() {
        return new ProgramExecutionState<>(name);
    }

    void run(Query query, ProgramExecutionState<ResultT> state) {
        Result result;
        do {
            ExecutionPhase phase = query.currentPhase();

            try {
                result = phase.evaluate(query);
            } catch (Throwable th) {
                // handles exception from synchronous part of phase evaluation

                if (shouldRetry(query, th)) {
                    continue;
                }

                query.setError(th);
                finalizeActiveProgram(query, state);

                return;
            }

            if (result.status() == Status.WAITING_FOR_COMPLETION) {
                CompletableFuture<Void> awaitFuture = result.await();

                assert awaitFuture != null;

                // reschedule only if required computation has not been done yet or it was completed exceptionally
                if (!awaitFuture.isDone() || awaitFuture.isCompletedExceptionally()) {
                    awaitFuture
                            .whenComplete((ignored, ex) -> {
                                if (ex != null) {
                                    ex = ExceptionUtils.unwrapCause(ex);

                                    // handles exception from asynchronous part of phase evaluation
                                    if (shouldRetry(query, ex)) {
                                        query.executor.execute(() -> run(query, state));
                                    } else {
                                        query.setError(ex);
                                        finalizeActiveProgram(query, state);
                                    }

                                    return;
                                }

                                query.executor.execute(() -> {
                                    if (advanceQuery(query, state)) {
                                        run(query, state);
                                    }
                                });
                            });
                    break;
                }
            }
        } while (advanceQuery(query, state));
    }

    private boolean shouldRetry(Query query, Throwable th) {
        try {
            if (errorHandler.test(query, th)) {
                return true;
            }
        } catch (Throwable throwableFromErrorHandler) {
            LOG.warn("Exception in error handler [queryId={}]", throwableFromErrorHandler, query.id);

            query.terminateExceptionally(th);
        }

        return false;
    }

    private static void finalizeActiveProgram(Query query, ProgramExecutionState<?> executionState) {
        ProgramExecutionHandle activeHandle = query.activeProgram.getAndSet(null);

        Throwable throwable = query.error.get();
        if (throwable != null) {
            // Set error as result of execution.
            executionState.notifyError(throwable);

            query.terminate();
        }

        executionState.programFinished.complete(null);
        assert activeHandle == executionState;
    }

    /**
     * Moves query to next phase.
     *
     * @param query Query to advance.
     * @return {@code true} if new state is not terminal (e.g. it does make sense to continue execution).
     */
    private boolean advanceQuery(Query query, ProgramExecutionState<ResultT> state) {
        ExecutionPhase phase = query.currentPhase();

        Transition transition = transitions.get(phase);

        assert transition != null : "Transition not found in program \"" + name + "\" for phase " + phase;

        transition.move(query);

        if (terminalPhase.test(query.currentPhase())) {
            ResultT result = this.result.apply(query);

            finalizeActiveProgram(query, state);

            if (!state.resultHolder.complete(result)) {
                assert state.resultHolder.isCompletedExceptionally();

                query.moveTo(ExecutionPhase.TERMINATED);
            }

            return false;
        }

        return true;
    }
}
