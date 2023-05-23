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
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.jetbrains.annotations.Nullable;

/**
 * Parse tree for {@code ALTER TABLE ... ALTER COLUMN ... SET/DROP DEFAULT} statement.
 */
public class IgniteSqlAlterColumnDefault extends IgniteSqlAlterColumnAction {
    /** DEFAULT expression. */
    private final SqlNode dflt;

    /** Constructor. */
    public IgniteSqlAlterColumnDefault(SqlParserPos pos, SqlNode dflt) {
        super(pos);

        this.dflt = dflt;
    }

    /**
     * Gets the new column DEFAULT expression.
     *
     * @return DEFAULT expression or {@code null} if the DEFAULT needs to be dropped.
     */
    public @Nullable SqlNode expression() {
        return dflt;
    }

    /** {@inheritDoc} */
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(dflt != null ? "SET" : "DROP");
        writer.keyword("DEFAULT");

        if (dflt != null) {
            dflt.unparse(writer, leftPrec, rightPrec);
        }
    }

    /** {@inheritDoc} */
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(dflt);
    }
}
