package org.apache.ignite.internal.sql.engine.exec.fsm;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.internal.sql.engine.exec.fsm.Result.Status;
import org.apache.ignite.internal.sql.engine.util.Commons;

class Program<ResultT> {
    private final String name;
    private final Map<ExecutionPhase, Transition> transitions;
    private final Predicate<ExecutionPhase> terminalPhase;
    private final Function<Query, ResultT> result;

    Program(
            String name,
            List<Transition> transitions,
            Predicate<ExecutionPhase> terminalPhase,
            Function<Query, ResultT> result
    ) {
        this.name = name;
        this.transitions = transitions.stream()
                .collect(Collectors.toMap(Transition::from, Function.identity()));
        this.terminalPhase = terminalPhase;
        this.result = result;
    }

    CompletableFuture<ResultT> run(Query query) {
        Result result;
        boolean needToSchedule;
        do {
            needToSchedule = false;
            ExecutionPhase phase = query.currentPhase();

            try {
                result = phase.evaluate(query);
            } catch (Throwable th) {
                // handles exception from synchronous part of phase evaluation

                query.onError(th);

                return Commons.cast(query.resultHolder);
            }

            if (result.status() == Status.WAITING_FOR_COMPLETION) {
                CompletableFuture<Void> awaitFuture = result.await();

                assert awaitFuture != null;

                // reschedule only if required computation has not been done yet or it was completed exceptionally
                if (!awaitFuture.isDone() || awaitFuture.isCompletedExceptionally()) {
                    needToSchedule = true;
                    break;
                }
            }
        } while (advanceQuery(query));

        if (needToSchedule) {
            CompletableFuture<Void> awaitFuture = result.await();

            assert awaitFuture != null;

            awaitFuture
                    .whenComplete((ignored, ex) -> {
                        if (ex != null) {
                            // handles exception from asynchronous part of phase evaluation
                            query.onError(ex);

                            return;
                        }

                        query.executor.execute(() -> {
                            if (advanceQuery(query)) {
                                run(query);
                            }
                        });
                    });
        }

        return Commons.cast(query.resultHolder);
    }

    /**
     * Moves query to next phase.
     * 
     * @param query Query to advance.
     * @return {@code true} if new state is not terminal (e.g. it does make sense to continue execution).
     */
    private boolean advanceQuery(Query query) {
        ExecutionPhase phase = query.currentPhase();

        Transition transition = transitions.get(phase);

        assert transition != null : "Transition not found in program \"" + name + "\" for phase " + phase;

        transition.move(query);

        if (terminalPhase.test(query.currentPhase())) {
            ResultT result = this.result.apply(query);

            query.resultHolder.complete(result);

            return false;
        }

        return true;
    } 
}
