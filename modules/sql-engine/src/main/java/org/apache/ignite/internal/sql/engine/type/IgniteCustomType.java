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

package org.apache.ignite.internal.sql.engine.type;

import java.lang.reflect.Type;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlTypeNameSpec;
import org.apache.ignite.sql.ColumnType;

/**
 * A base class for custom data types.
 * <p><b>Custom data type implementation check list.</b></p>
 * <p>
 *     Add a subclass that extends {@link IgniteCustomType}.
 * </p>
 * <ul>
 *     <li>Implement {@link IgniteCustomType#storageType()}.</li>
 *     <li>Implement {@link IgniteCustomType#nativeType()}.</li>
 *     <li>Implement {@link IgniteCustomType#columnType()}.</li>
 *     <li>Implement {@link IgniteCustomType#createWithNullability(boolean)}.</li>
 * </ul>
 * <p>
 * Update {@link IgniteTypeFactory}'s constructor to register your type.
 * </p>
 * <p>
 * Update type inference for dynamic parameters in {@link org.apache.ignite.internal.sql.engine.prepare.IgniteSqlValidator}.
 * </p>
 * <p>
 * Update {@link org.apache.ignite.internal.sql.engine.util.TypeUtils}:
 * </p>
 * <ul>
 *     <li>Update {@link org.apache.ignite.internal.sql.engine.util.TypeUtils#toInternal(ExecutionContext, Object, Type)}
 *     and {@link org.apache.ignite.internal.sql.engine.util.TypeUtils#fromInternal(ExecutionContext, Object, Type)}
 *     to adds assertions that check that a value has the same type as a {@link #storageType()}.</li>
 * </ul>
 * <p>
 * Update both {@link org.apache.ignite.internal.sql.engine.exec.exp.RexToLixTranslator} and
 * {@link org.apache.ignite.internal.sql.engine.exec.exp.ConverterUtils} to implement runtime routines for conversion
 * of your type from other data types if necessary.
 * </p>
 * Further steps:
 * <ul>
 *     <li>Update an SQL parser generator code to support your type - see DataTypeEx().</li>
 *     <li>Update JdbcDatabaseMetadata getTypeInfo</li>
 *     <li>Update {@link org.apache.ignite.internal.sql.engine.exec.exp.agg.Accumulators} if your type supports some aggregation functions.
 *     By default all custom data type support {@code COUNT}, {@code ANY_VALUE} and {@code ANY_VALUE}.</li>
 *     <li>Update serialisation/deserialisation code to store extra attributes.</li>
 * </ul>
 * <b>Update this documentation when you are going to change this procedure.</b>
 *
 * @see org.apache.ignite.internal.sql.engine.util.BaseQueryContext BaseQueryContext registers all custom types in the root schema.
*/
public abstract class IgniteCustomType extends RelDataTypeImpl {
    /** Nullable flag. */
    private final boolean nullable;

    /** Precision. **/
    private final int precision;

    /** Constructor. */
    protected IgniteCustomType(boolean nullable, int precision) {
        this.nullable = nullable;
        this.precision = precision;

        computeDigest();
    }

    /** Return the name of this type. **/
    public abstract String getTypeName();

    /**
     * Returns the storage type of this data type.
     * This method is called by {@link IgniteTypeFactory#getJavaClass(RelDataType)}
     * to provide types for a expression interpreter. Execution engine also relies on the fact that this
     * type is also used by {@link org.apache.ignite.internal.sql.engine.util.TypeUtils} in type conversions.
     *
     * @see org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactoryImpl
     * @see org.apache.ignite.internal.sql.engine.util.TypeUtils#toInternal(ExecutionContext, Object, Type)
     * @see org.apache.ignite.internal.sql.engine.util.TypeUtils#fromInternal(ExecutionContext, Object, Type)
     */
    public abstract Type storageType();

    /**
     * Returns the {@link NativeType} for this custom data type.
     * At the moment it serves the following purpose:
     * <ul>
     *     <li>
     *         Used by {@link IgniteTypeFactory#relDataTypeToNative(RelDataType)} to retrieve underlying
     *        {@link NativeType} for DDL queries.
     *     </li>
     *     <li>
     *         To retrieve a java type to perform type conversions by
     *         {@link org.apache.ignite.internal.sql.engine.exec.ExecutionServiceImpl}.
     *     </li>
     * </ul>
     */
    public abstract NativeType nativeType();

    /**
     * Returns the {@link ColumnType} of this data type. Provides type information for {@link org.apache.ignite.sql.ColumnMetadata}.
     */
    public abstract ColumnType columnType();

    /** {@inheritDoc} */
    @Override public final boolean isNullable() {
        return nullable;
    }

    /** {@inheritDoc} */
    @Override public final RelDataTypeFamily getFamily() {
        return SqlTypeFamily.ANY;
    }

    /** {@inheritDoc} */
    @Override public final SqlTypeName getSqlTypeName() {
        return SqlTypeName.ANY;
    }

    /** {@inheritDoc} */
    @Override
    public final int getPrecision() {
        return precision;
    }

    /** Creates an instance of this type with the specified nullability. **/
    public abstract IgniteCustomType createWithNullability(boolean nullable);

    /**
     * Creates an {@link SqlTypeNameSpec} for this custom data type, which is used as an argument for the CAST function.
     *
     * @return  an sql type name spec.
     */
    public final SqlTypeNameSpec createTypeNameSpec() {
        if (getPrecision() == PRECISION_NOT_SPECIFIED) {
            SqlIdentifier typeNameId = new SqlIdentifier(getTypeName(), SqlParserPos.ZERO);

            return new IgniteSqlTypeNameSpec(typeNameId, SqlParserPos.ZERO);
        } else {
            var typeNameId = new SqlIdentifier(getTypeName(), SqlParserPos.ZERO);
            var precision = SqlLiteral.createExactNumeric(Integer.toString(getPrecision()), SqlParserPos.ZERO);

            return new IgniteSqlTypeNameSpec(typeNameId, precision, SqlParserPos.ZERO);
        }
    }
}
