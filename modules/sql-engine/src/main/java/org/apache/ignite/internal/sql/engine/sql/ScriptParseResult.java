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
import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.tostring.S;

/**
 * Result of a parsing SQL string that multiple statements.
 */
public final class ScriptParseResult extends ParseResult {

    /**
     * Parse operation is expected to return one or multiple statements.
     */
    public static final ParseMode<ScriptParseResult> MODE = new ParseMode<>() {
        @Override
        ScriptParseResult createResult(List<SqlNode> list, int dynamicParamsCount) {
            return new ScriptParseResult(list, dynamicParamsCount);
        }
    };

    private final List<SqlNode> statements;

    /**
     * Constructor.
     *
     * @param statements A list of parsed statements.
     * @param dynamicParamsCount The number of dynamic parameters.
     */
    public ScriptParseResult(List<SqlNode> statements, int dynamicParamsCount) {
        super(dynamicParamsCount);
        this.statements = statements;
    }

    /** Returns a list of parsed statements. */
    @Override
    public List<SqlNode> statements() {
        return statements;
    }

    /** {@inheritDoc} **/
    @Override
    public String toString() {
        return S.toString(ScriptParseResult.class, this);
    }
}
