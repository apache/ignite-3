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

import java.util.function.Predicate;
import org.apache.calcite.sql.SqlNode;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;

/**
 * Common methods to verify parsing of the DDL command.
 */
public abstract class AbstractDdlParserTest {
    /**
     * Parses a given statement and returns a resulting AST.
     *
     * @param stmt Statement to parse.
     * @return An AST.
     */
    protected SqlNode parse(String stmt) {
        StatementParseResult parseResult = IgniteSqlParser.parse(stmt, StatementParseResult.MODE);
        return parseResult.statement();
    }

    /**
     * Matcher to verify that an object of the expected type and matches the given predicate.
     *
     * @param desc Description for this matcher.
     * @param cls  Expected class to verify the object is instance of.
     * @param pred Addition check that would be applied to the object.
     * @return {@code true} in case the object if instance of the given class and matches the predicat.
     */
    protected <T> Matcher<T> ofTypeMatching(String desc, Class<T> cls, Predicate<T> pred) {
        return new CustomMatcher<>(desc) {
            /** {@inheritDoc} */
            @Override
            public boolean matches(Object item) {
                return item != null && cls.isAssignableFrom(item.getClass()) && pred.test((T) item);
            }
        };
    }
}
