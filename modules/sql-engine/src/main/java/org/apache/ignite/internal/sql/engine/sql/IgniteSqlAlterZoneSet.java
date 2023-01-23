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
import java.util.Objects;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

/**
 * Parse tree for {@code ALTER ZONE SET} statement.
 */
public class IgniteSqlAlterZoneSet extends IgniteAbstractSqlAlterZone {

    private final SqlNodeList optionList;

    /** Constructor. */
    public IgniteSqlAlterZoneSet(SqlParserPos pos, SqlIdentifier name, SqlNodeList optionList, boolean ifExists) {
        super(pos, name, ifExists);
        this.optionList = Objects.requireNonNull(optionList, "optionList");
    }

    /** {@inheritDoc} */
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, optionList);
    }

    /** {@inheritDoc} */
    @Override
    protected void unparseAlterZoneOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SET");

        optionList.unparse(writer, 0, 0);
    }

    /** The list of modification options. **/
    public SqlNodeList alterOptionsList() {
        return optionList;
    }
}
