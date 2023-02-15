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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.ExecutionServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.exp.ConverterUtils;
import org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactoryImpl;
import org.apache.ignite.internal.sql.engine.exec.exp.RexToLixTranslator;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.Accumulators;
import org.apache.ignite.internal.sql.engine.prepare.IgniteSqlValidator;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlTypeNameSpec;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;

/**
 * A base class for custom data types.
 *
 * <p><b>Custom data type implementation check list.</b>
 *
 * <p>Add a subclass that extends {@link IgniteCustomType}.
 * <ul>
 *     <li>Implement {@link IgniteCustomType#storageType()} - storage type must implement {@link Comparable}.
 *     This is a requirement imposed by calcite's row-expressions implementation.
 *     (see {@link org.apache.ignite.internal.sql.engine.rex.IgniteRexBuilder IgniteRexBuilder}).</li>
 *     <li>Implement {@link IgniteCustomType#nativeType()}.</li>
 *     <li>Implement {@link IgniteCustomType#columnType()}.</li>
 *     <li>Implement {@link IgniteCustomType#createWithNullability(boolean)}.</li>
 *     <li>Implement {@link IgniteCustomType#getFamily()}. In the simple case implementation must return
 *     an instance of {@link IgniteCustomTypeFamily} that is shared by all instances of this type.</li>
 * </ul>
 *
 * <p>Code base contains comments that start with {@code IgniteCustomType:} to provide extra information.
 *
 * <p>Update {@link IgniteTypeFactory}'s constructor to register your type.
 *
 * <p>Update type inference for dynamic parameters in {@link IgniteSqlValidator}.
 *
 * <p>Update {@link TypeUtils}:
 * <ul>
 *     <li>Update {@link TypeUtils#toInternal(ExecutionContext, Object, Type)} and
 *     {@link TypeUtils#fromInternal(ExecutionContext, Object, Type)} to add assertions that check
 *     that a value has the same type as a {@link #storageType()}.</li>
 * </ul>
 *
 * <p>Update both {@link RexToLixTranslator RexToLitTranslator} and
 * {@link ConverterUtils} to implement runtime routines for conversion
 * of your type from other data types if necessary.
 *
 * <p>Further steps:
 * <ul>
 *     <li>Update an SQL parser generator code to support your type - see {@code DataTypeEx()}.</li>
 *     <li>Update {@link Accumulators}
 *     if your type supports some aggregation functions.
 *     By default all custom data type support {@code COUNT} and {@code ANY_VALUE}.</li>
 *     <li>Update serialisation/deserialisation in {@code RelJson} to store extra attributes if necessary.</li>
 *     <li>There probably some methods in {@link IgniteTypeSystem} that maybe subject to change
 *     when a custom data type is implemented.</li>
 * </ul>
 *
 * <p>Client code/JDBC:
 * <ul>
 *     <li>Update {@code JdbcDatabaseMetadata::getTypeInfo} to return information about your type.</li>
 *     <li>Update {@code JdbcColumnMeta::typeName} to return the correct name for your time.</li>
 * </ul>
 *
 * <p><b>Update this documentation when you are going to change this procedure.</b>
 *
*/
public abstract class IgniteCustomType<StorageT extends Comparable<StorageT>> extends RelDataTypeImpl {

    private final Class<StorageT> storageType;

    private final boolean nullable;

    private final int precision;

    /** Constructor. */
    protected IgniteCustomType(Class<StorageT> storageType, boolean nullable, int precision) {
        this.storageType = storageType;
        this.nullable = nullable;
        this.precision = precision;

        computeDigest();
    }

    /** Returns the name of this type. **/
    public abstract String getCustomTypeName();

    /**
     * Returns the storage type of this data type.
     *
     * <p>This method is called by {@link IgniteTypeFactory#getJavaClass(RelDataType)}
     * to provide types for a expression interpreter. Execution engine also relies on the fact that this
     * type is also used by {@link TypeUtils TypeUtils} in type conversions.
     *
     * @see ExpressionFactoryImpl
     * @see TypeUtils#toInternal(ExecutionContext, Object, Type)
     * @see TypeUtils#fromInternal(ExecutionContext, Object, Type)
     */
    public final Type storageType() {
        return storageType;
    }

    /**
     * Returns the {@link NativeType} for this custom data type.
     *
     * <p>At the moment it serves the following purpose:
     * <ul>
     *     <li>
     *         Used by {@link IgniteTypeFactory#relDataTypeToNative(RelDataType)} to retrieve underlying
     *        {@link NativeType} for DDL queries.
     *     </li>
     *     <li>
     *         To retrieve a java type to perform type conversions by
     *         {@link ExecutionServiceImpl}.
     *     </li>
     * </ul>
     */
    public abstract NativeType nativeType();

    /**
     * Returns the {@link ColumnType} of this data type. Provides type information for {@link ColumnMetadata}.
     */
    public abstract ColumnType columnType();

    /** {@inheritDoc} */
    @Override public final boolean isNullable() {
        return nullable;
    }

    /** {@inheritDoc} */
    @Override public abstract RelDataTypeFamily getFamily();

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
    public abstract IgniteCustomType<StorageT> createWithNullability(boolean nullable);

    /**
     * Creates an {@link SqlTypeNameSpec} for this custom data type, which is used as an argument for the CAST function.
     *
     * @return  An SQL type name spec.
     */
    public final SqlTypeNameSpec createTypeNameSpec() {
        if (getPrecision() == PRECISION_NOT_SPECIFIED) {
            SqlIdentifier typeNameId = new SqlIdentifier(getCustomTypeName(), SqlParserPos.ZERO);

            return new IgniteSqlTypeNameSpec(typeNameId, SqlParserPos.ZERO);
        } else {
            var typeNameId = new SqlIdentifier(getCustomTypeName(), SqlParserPos.ZERO);
            var precision = SqlLiteral.createExactNumeric(Integer.toString(getPrecision()), SqlParserPos.ZERO);

            return new IgniteSqlTypeNameSpec(typeNameId, precision, SqlParserPos.ZERO);
        }
    }
}
