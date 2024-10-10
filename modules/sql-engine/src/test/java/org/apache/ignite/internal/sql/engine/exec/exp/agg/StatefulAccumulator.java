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

package org.apache.ignite.internal.sql.engine.exec.exp.agg;

import java.util.function.Supplier;
import org.jetbrains.annotations.Nullable;

/**
 * A helper class for testing accumulator functions.
 */
public final class StatefulAccumulator {

    private final Accumulator accumulator;

    private final AccumulatorsState state = new AccumulatorsState(1);

    private final AccumulatorsState result = new AccumulatorsState(1);

    public StatefulAccumulator(Supplier<? extends Accumulator> supplier) {
        this(supplier.get());
    }

    public StatefulAccumulator(Accumulator accumulator) {
        this.accumulator = accumulator;
    }

    public void add(Object... args) {
        accumulator.add(state, args);
    }

    public @Nullable Object end() {
        accumulator.end(state, result);
        return result.get();
    }
}
