/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.util;

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import java.util.List;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rex.RexNode;
import org.jetbrains.annotations.Nullable;

/**
 * Index conditions and bounds holder. Conditions are not printed to terms (serialized). They are used only to calculate selectivity.
 */
public class IndexConditions {
    private final List<RexNode> lowerCond;

    private final List<RexNode> upperCond;

    private final List<RexNode> lowerBound;

    private final List<RexNode> upperBound;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public IndexConditions() {
        this(null, null, null, null);
    }

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public IndexConditions(
            @Nullable List<RexNode> lowerCond,
            @Nullable List<RexNode> upperCond,
            @Nullable List<RexNode> lowerBound,
            @Nullable List<RexNode> upperBound
    ) {
        this.lowerCond = lowerCond;
        this.upperCond = upperCond;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public IndexConditions(RelInput input) {
        lowerCond = null;
        upperCond = null;
        lowerBound = input.get("lower") == null ? null : input.getExpressionList("lower");
        upperBound = input.get("upper") == null ? null : input.getExpressionList("upper");
    }

    /**
     * Get lower index condition.
     */
    public List<RexNode> lowerCondition() {
        return lowerCond;
    }

    /**
     * Get upper index condition.
     */
    public List<RexNode> upperCondition() {
        return upperCond;
    }

    /**
     * Get lower index bounds (a row with values at the index columns).
     */
    public List<RexNode> lowerBound() {
        return lowerBound;
    }

    /**
     * Get upper index bounds (a row with values at the index columns).
     */
    public List<RexNode> upperBound() {
        return upperBound;
    }

    /**
     * Keys.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public IntSet keys() {
        if (upperBound == null && lowerBound == null) {
            return IntSets.EMPTY_SET;
        }

        IntSet keys = new IntOpenHashSet();

        int cols = lowerBound != null ? lowerBound.size() : upperBound.size();

        for (int i = 0; i < cols; ++i) {
            if (upperBound != null && RexUtils.isNotNull(upperBound.get(i))
                    || lowerBound != null && RexUtils.isNotNull(lowerBound.get(i))) {
                keys.add(i);
            }
        }

        return IntSets.unmodifiable(keys);
    }

    /**
     * Describes index bounds.
     *
     * @param pw Plan writer.
     * @return Plan writer for fluent-explain pattern.
     */
    public RelWriter explainTerms(RelWriter pw) {
        return pw
                .itemIf("lower", lowerBound, !nullOrEmpty(lowerBound))
                .itemIf("upper", upperBound, !nullOrEmpty(upperBound));
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return "IndexConditions{"
                + "lowerCond=" + lowerCond
                + ", upperCond=" + upperCond
                + ", lowerBound=" + lowerBound
                + ", upperBound=" + upperBound
                + '}';
    }
}
