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

package org.apache.ignite.internal.sql.engine.util;

import static org.apache.ignite.internal.sql.engine.hint.IgniteHint.EXPAND_DISTINCT_AGG;

import java.util.Arrays;
import java.util.StringJoiner;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.ignite.internal.sql.engine.hint.IgniteHint;

/**
 * Hint util methods.
 */
public class HintUtils {
    private HintUtils() {
        // No-op.
    }

    /**
     * Return {@code true} if the EXPAND_DISTINCT_AGG hint presents in provided logical aggregate and aggregate contains distinct clause.
     *
     * @param rel Logical aggregate to check on expand distinct aggregate hint.
     */
    public static boolean isExpandDistinctAggregate(LogicalAggregate rel) {
        return rel.getHints().stream()
                .anyMatch(r -> r.hintName.equals(EXPAND_DISTINCT_AGG.name()))
                && rel.getAggCallList().stream().anyMatch(AggregateCall::isDistinct);
    }

    /**
     * Generate string representation of the hint together with a list of parameters. Can be used as is in query.
     *
     * @return String representation of a hint together with a list of parameters..
     */
    public static String toHint(IgniteHint hint, String... params) {
        StringJoiner joiner = new StringJoiner(",", "/*+ " + hint.name() + "(", ") */");

        if (params != null) {
            assert hint.paramSupport();
            Arrays.stream(params).forEach(p -> joiner.add("'" + p + "'"));
        }

        return joiner.toString();
    }
}
