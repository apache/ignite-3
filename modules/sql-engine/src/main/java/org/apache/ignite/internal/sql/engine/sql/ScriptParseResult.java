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
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;
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
        private final SqlDynamicParamsAdjuster dynamicParamsAdjuster = new SqlDynamicParamsAdjuster();

        @Override
        ScriptParseResult createResult(List<SqlNode> list, int dynamicParamsCount) {
            if (list.size() == 1) {
                return new ScriptParseResult(List.of(new StatementParseResult(list.get(0), dynamicParamsCount)), dynamicParamsCount);
            }

            List<StatementParseResult> results = list.stream()
                    .map(node -> makeParseResultFromTree(node, dynamicParamsCount))
                    .collect(Collectors.toList());

            return new ScriptParseResult(results, dynamicParamsCount);
        }

        private StatementParseResult makeParseResultFromTree(SqlNode tree, int dynamicParamsCount) {
            if (dynamicParamsCount == 0) {
                return new StatementParseResult(tree, 0);
            }

            SqlNode newTree = dynamicParamsAdjuster.visitNode(tree);

            assert newTree != null;

            return new StatementParseResult(newTree, dynamicParamsAdjuster.count);
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
     * Adjusts the dynamic parameter indexes to match the single statement parameter indexes.
     */
    private static final class SqlDynamicParamsAdjuster extends SqlShuttle {
        private int count;

        @Override
        public SqlNode visit(SqlDynamicParam param) {
            return new SqlDynamicParam(count++, param.getParserPosition());
        }

        @Override
        public @Nullable SqlNode visitNode(SqlNode n) {
            count = 0;

            return super.visitNode(n);
        }
    }

    /** {@inheritDoc} **/
    @Override
    public String toString() {
        return S.toString(ScriptParseResult.class, this);
    }
}
