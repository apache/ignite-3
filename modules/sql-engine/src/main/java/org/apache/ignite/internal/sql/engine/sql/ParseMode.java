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

/**
 * Parse mode determines the expected result of parse operation.
 *
 * <p>For example, {@link StatementParseResult#MODE statement mode} guarantees that a parse operation returns a single statement
 * and when input contains multiple statements, it throws an exception.
 *
 * @param <T>  A class of parse result.
 * @see StatementParseResult#MODE
 * @see ScriptParseResult#MODE
 */
public abstract class ParseMode<T extends ParseResult> {

    // Do not allow subclasses outside this package.
    ParseMode() {

    }

    /**
     * Constructs an instance of ParseResult from the given list of statements.
     *
     * @param list A list of statements.
     * @param dynamicParamsCount The number of dynamic parameters in the given input.
     * @return a result.
     */
    abstract T createResult(List<SqlNode> list, int dynamicParamsCount);
}
