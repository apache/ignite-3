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

import java.io.Serializable;
import java.util.List;
import java.util.function.IntFunction;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AccumulatorWrapper.StateOutput;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;

/**
 * Accumulator interface.
 */
public interface Accumulator extends Serializable {
    void add(Object... args);

    void apply(Accumulator other);

    Object end();

    List<RelDataType> argumentTypes(IgniteTypeFactory typeFactory);

    RelDataType returnType(IgniteTypeFactory typeFactory);

    void applyState(IntFunction<Object> state);

    List<RelDataType> state(IgniteTypeFactory typeFactory);

    void write(StateOutput output);
}
