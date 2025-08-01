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

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;

/**
 * Accumulator interface.
 */
public interface Accumulator {

    /**
     * Updates this accumulator.
     *
     * @param state state of the accumulator.
     * @param args arguments.
     */
    void add(AccumulatorsState state, Object[] args);

    /**
     * Computes result of this accumulator.
     *
     * @param state Accumulator state.
     * @param result Result holder.
     */
    void end(AccumulatorsState state, AccumulatorsState result);

    /**
     * Returns types of arguments for this accumulator.
     *
     * @param typeFactory Type factory.
     * @return List of argument types.
     */
    List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory);

    /**
     * Returns a result type for this accumulator.
     *
     * @param typeFactory Type factory.
     * @return A result type.
     */
    RelDataType returnType(IgniteTypeFactory typeFactory);
}
