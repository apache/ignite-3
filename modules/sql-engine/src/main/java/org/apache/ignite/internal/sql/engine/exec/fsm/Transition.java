package org.apache.ignite.internal.sql.engine.exec.fsm;

import java.util.function.Function;

class Transition {
    private final ExecutionPhase from;
    private final Function<Query, ExecutionPhase> to;

     Transition(ExecutionPhase from, Function<Query, ExecutionPhase> to) {
        this.from = from;
        this.to = to;
    }

    ExecutionPhase from() {
         return from; 
    }

    void move(Query query) {
         ExecutionPhase newPhase = to.apply(query);

         query.moveTo(newPhase); 
    }
}
