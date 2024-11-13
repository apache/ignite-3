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

import java.util.function.Function;

/**
 * Describes a transition in query FSM.
 * 
 * <p>This class encapsulates logic of choosing particular destination by considering current state of the {@link Query}.
 */
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

        ExecutionPhase current = query.currentPhase();

        assert current == from : current;

        query.moveTo(newPhase);
    }
}
