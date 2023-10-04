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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 *  Parse tree for {@code START TRANSACTION } statement.
 */
public class IgniteSqlStartTransaction extends SqlCall {

    /** Start transaction operator. */
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("START TRANSACTION", SqlKind.OTHER);

    private final boolean readOnly;

    /** Creates a IgniteSqlStartTransaction. */
    public IgniteSqlStartTransaction(SqlParserPos pos, boolean readOnly) {
        super(pos);

        this.readOnly = readOnly;
    }

    /** Returns {@code true} if transaction access mode is read-only mode. Otherwise returns {@code false} .*/
    public boolean isReadOnly() {
        return readOnly;
    }

    /** {@inheritDoc} */
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    /** {@inheritDoc} */
    @Override
    public List<SqlNode> getOperandList() {
        return List.of();
    }

    /** {@inheritDoc} */
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(getOperator().getName());

        if (readOnly) {
            writer.keyword("READ");
            writer.keyword("ONLY");
        } else {
            writer.keyword("READ");
            writer.keyword("WRITE");
        }
    }
}
