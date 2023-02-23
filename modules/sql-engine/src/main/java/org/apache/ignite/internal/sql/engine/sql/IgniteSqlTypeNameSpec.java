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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Litmus;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Type name specification that allows to derive custom data types.
 *
 * @see org.apache.ignite.internal.sql.engine.type.IgniteCustomType
 */
public final class IgniteSqlTypeNameSpec extends SqlTypeNameSpec {

    /** Undefined precision. Use the same value as apache calcite. **/
    private static final int UNDEFINED_PRECISION = -1;

    /** Precision, if specified. **/
    private final int precision;

    /**
     * Creates a {@code SqlTypeNameSpec}.
     *
     * @param name Name of the type
     * @param precision -1 if not specified.
     * @param pos Parser position, must not be null
     */
    public IgniteSqlTypeNameSpec(SqlIdentifier name, @Nullable SqlLiteral precision, SqlParserPos pos) {
        super(name, pos);
        this.precision = precision != null ? precision.intValue(true) : UNDEFINED_PRECISION;
    }

    /**
     * Creates a {@code SqlTypeNameSpec} for the specified type with undefined precision.
     *
     * @param name Name of the type
     * @param pos Parser position, must not be null
     */
    public IgniteSqlTypeNameSpec(SqlIdentifier name, SqlParserPos pos) {
        this(name, null, pos);
    }

    /** {@inheritDoc} **/
    @Override
    public RelDataType deriveType(SqlValidator validator) {
        IgniteTypeFactory typeFactory = (IgniteTypeFactory) validator.getTypeFactory();
        return typeFactory.createCustomType(getTypeName().getSimple(), precision);
    }

    /** {@inheritDoc} **/
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(getTypeName().getSimple());

        if (precision != -1) {
            SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
            writer.print(precision);
            writer.endList(frame);
        }
    }

    /** {@inheritDoc} **/
    @Override
    public boolean equalsDeep(SqlTypeNameSpec spec, Litmus litmus) {
        if (!(spec instanceof IgniteSqlTypeNameSpec)) {
            return litmus.fail("{} != {}", this, spec);
        } else {
            IgniteSqlTypeNameSpec that = (IgniteSqlTypeNameSpec) spec;
            if (that.precision != precision) {
                return litmus.fail("{} != {}", this, spec);
            }
            return litmus.succeed();
        }
    }
}
