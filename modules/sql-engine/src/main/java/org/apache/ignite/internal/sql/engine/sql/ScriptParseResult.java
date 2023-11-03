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

package org.apache.ignite.internal.sql.engine.sql;

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Result of parsing SQL string that multiple statements.
 */
public final class ScriptParseResult extends ParseResult {

    /**
     * Parse operation is expected to return one or multiple statements.
     */
    public static final ParseMode<ScriptParseResult> MODE = new ParseMode<>() {
        @Override
        ScriptParseResult createResult(List<SqlNode> list, int dynamicParamsCount) {
            if (list.size() == 1) {
                return new ScriptParseResult(List.of(new StatementParseResult(list.get(0), dynamicParamsCount)), dynamicParamsCount);
            }

            SqlDynamicParamsCounter paramsCounter = dynamicParamsCount == 0 ? null : new SqlDynamicParamsCounter();
            List<StatementParseResult> results = list.stream()
                    .map(node -> new StatementParseResult(node, paramsCounter == null ? 0 : paramsCounter.forNode(node)))
                    .collect(Collectors.toList());

            return new ScriptParseResult(results, dynamicParamsCount);
        }
    };

    private final List<StatementParseResult> results;

    /**
     * Constructor.
     *
     * @param results A list of parsing results.
     * @param dynamicParamsCount The number of dynamic parameters.
     */
    private ScriptParseResult(List<StatementParseResult> results, int dynamicParamsCount) {
        super(dynamicParamsCount);
        this.results = results;
    }

    /** Returns a list of parsed statements. */
    public List<StatementParseResult> results() {
        return results;
    }

    /**
     * Counts the number of {@link SqlDynamicParam} nodes in the tree.
     */
    @NotThreadSafe
    static class SqlDynamicParamsCounter extends SqlBasicVisitor<Object> {
        int count;

        @Override
        public @Nullable Object visit(SqlDynamicParam param) {
            count++;

            return null;
        }

        int forNode(SqlNode node) {
            count = 0;

            this.visitNode(node);

            return count;
        }
    }

    /** {@inheritDoc} **/
    @Override
    public String toString() {
        return S.toString(ScriptParseResult.class, this);
    }
}
