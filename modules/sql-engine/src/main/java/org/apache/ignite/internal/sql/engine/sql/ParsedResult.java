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

import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.sql.engine.SqlQueryType;

/**
 * Result of the parse.
 *
 * @see ParserService
 */
public interface ParsedResult {
    /** Returns the type of the query. */
    SqlQueryType queryType();

    /** Returns the original query string sent to the parser service, based on which this result was created. */
    String originalQuery();

    /**
     * Returns the query string in a normal form.
     *
     * <p>That is, with keywords converted to upper case, and all non-quoted identifiers converted to upper case as well
     * and wrapped with double quotes.
     */
    String normalizedQuery();

    /** Returns the count of the dynamic params (specified by question marks in the query text) used in the query. */
    int dynamicParamsCount();

    /** Returns the syntax tree of the query according to the grammar rules. */
    SqlNode parsedTree();
}
