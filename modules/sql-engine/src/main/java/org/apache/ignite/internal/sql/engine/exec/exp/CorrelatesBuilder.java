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

package org.apache.ignite.internal.sql.engine.exec.exp;

import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.ignite.internal.sql.engine.exec.exp.RexToLixTranslator.InputGetter;

class CorrelatesBuilder extends RexShuttle {
    private final Expression ctx;

    private Map<String, InputGetter> correlates;

    CorrelatesBuilder(BlockBuilder builder, Expression ctx, Expression hnd) {
        this.ctx = ctx;
    }

    public Function1<String, InputGetter> build(Iterable<RexNode> nodes) {
        try {
            for (RexNode node : nodes) {
                if (node != null) {
                    node.accept(this);
                }
            }

            return correlates == null ? null : correlates::get;
        } finally {
            correlates = null;
        }
    }

    /** {@inheritDoc} */
    @Override
    public RexNode visitCorrelVariable(RexCorrelVariable variable) {
        if (correlates == null) {
            correlates = new HashMap<>();
        }

        InputGetter inputGetter = new CorrelatedValueGetter(ctx, variable);

        correlates.put(variable.getName(), inputGetter);

        return variable;
    }
}
