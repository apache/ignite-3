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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Base class for SQL operators.
 */
public abstract class IgniteSqlSpecialOperator extends SqlSpecialOperator {

    /** Constructor. */
    public IgniteSqlSpecialOperator(String name, SqlKind kind) {
        super(name, kind);
    }

    /** {@inheritDoc} */
    @Override
    public abstract SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands);
}
