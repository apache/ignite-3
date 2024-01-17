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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
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

    /**
     * @return Hints filtered with {@code hintDefs} and suitable for {@code rel}.
     * @see HintStrategyTable#apply(List, RelNode)
     * @see #filterHints(RelNode, Collection, List)
     */
    public static List<RelHint> hints(RelNode rel, IgniteHint... hints) {
        return rel.getCluster().getHintStrategies()
                .apply(filterHints(rel, allRelHints(rel), Arrays.asList(hints)), rel);
    }

    /**
     * @return Hints of {@code rel} if it is a {@code Hintable}. If is not or has no hints, empty collection.
     * @see Hintable#getHints()
     */
    public static List<RelHint> allRelHints(RelNode rel) {
        return rel instanceof Hintable ? ((Hintable) rel).getHints() : Collections.emptyList();
    }

    /**
     * @return Distinct hints within {@code hints} filtered with {@code hintDefs}, {@link HintOptionsChecker} and
     * removed inherit pathes.
     * @see HintOptionsChecker
     * @see RelHint#inheritPath
     */
    private static List<RelHint> filterHints(RelNode rel, Collection<RelHint> hints, List<IgniteHint> hintDefs) {
        Set<String> requiredHintDefs = hintDefs.stream().map(Enum::name).collect(Collectors.toSet());

        List<RelHint> res = hints.stream().filter(h -> requiredHintDefs.contains(h.hintName))
                .map(h -> {
                    RelHint.Builder rb = RelHint.builder(h.hintName);

                    if (!h.listOptions.isEmpty()) {
                        rb.hintOptions(h.listOptions);
                    } else if (!h.kvOptions.isEmpty()) {
                        rb.hintOptions(h.kvOptions);
                    }

                    return rb.build();
                }).distinct().collect(Collectors.toList());

        // Validate hint options.
//        Iterator<RelHint> it = res.iterator();
//
//        while (it.hasNext()) {
//            RelHint hint = it.next();
//
//            String optsErr = IgniteHint.valueOf(hint.hintName).optionsChecker().apply(hint);
//
//            if (!F.isEmpty(optsErr)) {
//                skippedHint(rel, hint, optsErr);
//
//                it.remove();
//            }
//        }

        return res;
    }
}
