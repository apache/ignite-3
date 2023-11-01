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

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.sql.ParserServiceImpl.ParsedResultImpl;
import org.apache.ignite.internal.sql.engine.util.Commons;

/**
 * An implementation of {@link ParserService} for parsing multi-statements, this service does not cache parsing results.
 */
public class ScriptParserService implements ParserService<List<ParsedResult>> {
    /** {@inheritDoc} */
    @Override
    public List<ParsedResult> parse(String query) {
        ScriptParseResult parsedStatement = IgniteSqlParser.parse(query, ScriptParseResult.MODE);
        List<ParsedResult> results = new ArrayList<>(parsedStatement.results().size());

        for (StatementParseResult result : parsedStatement.results()) {
            SqlNode parsedTree = result.statement();
            SqlQueryType queryType = Commons.getQueryType(parsedTree);
            String normalizedQuery = parsedTree.toString();

            assert queryType != null : normalizedQuery;

            results.add(new ParsedResultImpl(
                    queryType,
                    normalizedQuery,
                    normalizedQuery,
                    result.dynamicParamsCount(),
                    () -> parsedTree
            ));
        }

        return results;
    }
}
