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

import static org.apache.ignite.internal.sql.engine.sql.IgniteSqlZoneOptionV2.OPTIONS_MAPPING;

import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriter.FrameTypeEnum;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Litmus;
import org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum;
import org.checkerframework.checker.nullness.qual.Nullable;

/** An AST node representing option in CREATE ZONE statement responsible for storage profiles. */
public class IgniteSqlStorageProfile extends SqlCall {
    /** Storage profile operator. */
    protected static class Operator extends IgniteSqlSpecialOperator {

        /** Constructor. */
        protected Operator() {
            super("StorageProfiles", SqlKind.OTHER);
        }

        /** {@inheritDoc} */
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
            return new IgniteSqlStorageProfile((SqlIdentifier) operands[0], (SqlNodeList) operands[1], pos);
        }
    }

    private static final SqlOperator OPERATOR = new Operator();

    /** Profile key. */
    private final SqlIdentifier key;

    /** Profile values. */
    private final SqlNodeList values;

    /** Creates {@link IgniteSqlStorageProfile}. */
    public IgniteSqlStorageProfile(SqlIdentifier key, SqlNodeList values, SqlParserPos pos) {
        super(pos);

        assert key.isSimple() : key;

        this.key = key;
        this.values = values;
    }

    /** {@inheritDoc} */
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    /** {@inheritDoc} */
    @Override
    public List<SqlNode> getOperandList() {
        return List.of(key, values);
    }

    /** {@inheritDoc} */
    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new IgniteSqlStorageProfile(key, values, pos);
    }

    /** {@inheritDoc} */
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.identifier(OPTIONS_MAPPING.get(ZoneOptionEnum.STORAGE_PROFILES), false);

        SqlWriter.Frame frame = writer.startList(FrameTypeEnum.SIMPLE, "[", "]");
        for (SqlNode c : values) {
            writer.sep(",");
            c.unparse(writer, 0, 0);
        }
        writer.endList(frame);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus) {
        if (!(node instanceof IgniteSqlStorageProfile)) {
            return litmus.fail("{} != {}", this, node);
        }

        IgniteSqlStorageProfile that = (IgniteSqlStorageProfile) node;
        if (key != that.key) {
            return litmus.fail("{} != {}", this, node);
        }

        return values.equalsDeep(that.values, litmus);
    }

    /**
     * Get profile key.
     */
    public SqlIdentifier key() {
        return key;
    }

    /**
     * Get profile values.
     */
    public SqlNodeList value() {
        return values;
    }
}
