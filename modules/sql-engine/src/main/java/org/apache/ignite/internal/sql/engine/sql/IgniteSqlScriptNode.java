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
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@code IgniteSqlScriptNode} is a list of SQL statements with the number of dynamic parameters in it.
 */
public final class IgniteSqlScriptNode extends SqlNodeList {

    /** An empty SQL script. */
    public static final IgniteSqlScriptNode EMPTY = new IgniteSqlScriptNode(SqlNodeList.EMPTY, 0);

    private final int dynamicParamsCount;

    /** Constructor. **/
    public IgniteSqlScriptNode(SqlNodeList nodes, int dynamicParamsCount) {
        super(nodes, nodes.getParserPosition());
        this.dynamicParamsCount = dynamicParamsCount;
    }

    /** The number of dynamic parameters in an SQL script. **/
    public int dynamicParamsCount() {
        return dynamicParamsCount;
    }

    /** This method is not supported for {@link IgniteSqlScriptNode}. **/
    @Override
    public SqlNodeList clone(SqlParserPos pos) {
        throw new UnsupportedOperationException("IgniteSqlScriptNode::clone is not supported");
    }

    /** This method is not supported for {@link IgniteSqlScriptNode}. **/
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        throw new UnsupportedOperationException("IgniteSqlScriptNode::unparse is not supported");
    }

    /** This method is not supported for {@link IgniteSqlScriptNode}. **/
    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        throw new UnsupportedOperationException("IgniteSqlScriptNode::validate is not supported");
    }

    /** This method is not supported for {@link IgniteSqlScriptNode}. **/
    @Override
    public boolean equalsDeep(@Nullable SqlNode node, Litmus litmus) {
        throw new UnsupportedOperationException("IgniteSqlScriptNode::equalsDeep is not supported");
    }

    /** This method is not supported for {@link IgniteSqlScriptNode}. **/
    @Override
    public void validateExpr(SqlValidator validator, SqlValidatorScope scope) {
        throw new UnsupportedOperationException("IgniteSqlScriptNode::validateExpr is not supported");
    }
}
