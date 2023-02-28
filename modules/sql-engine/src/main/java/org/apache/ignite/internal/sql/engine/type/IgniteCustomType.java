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

import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.Accumulators;
import org.apache.ignite.internal.sql.engine.prepare.IgniteSqlValidator;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlTypeNameSpec;

/**
 * A base class for custom data types.
 *
 * <p><b>Custom data type implementation check list.</b>
 *
 * <p>Create a subclass of {@link IgniteCustomType}.
 *
 * <p>Define {@link IgniteCustomTypeSpec type spec} for your type that describes the following properties:
 * <ul>
 *     <li>{@link IgniteCustomTypeSpec#typeName() type name}.</li>
 *     <li>{@link IgniteCustomTypeSpec#nativeType() native type}.</li>
 *     <li>{@link IgniteCustomTypeSpec#columnType() column type}.</li>
 *     <li>{@link IgniteCustomTypeSpec#storageType() storage type}.</li>
 *     <li>{@link IgniteCustomTypeSpec#castFunction() cast function}.
 *     See {@link IgniteCustomTypeSpec#getCastFunction(Class, String) getCastFunction}.
 *     </li>
 * </ul>
 *
 * <p>Code base contains comments that start with {@code IgniteCustomType:} to provide extra information.
 *
 * <p>Update {@link IgniteTypeFactory}'s constructor to register your type and to specify type coercion rules.
 *
 * <p>Update type inference for dynamic parameters in {@link IgniteSqlValidator}.
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
 * <p>Type conversion is implemented in {@code CustomTypesConversion} and uses rules defined for your custom type.
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
public abstract class IgniteCustomType extends RelDataTypeImpl {

    private final IgniteCustomTypeSpec spec;

    private final boolean nullable;

    private final int precision;

    /** Constructor. */
    protected IgniteCustomType(IgniteCustomTypeSpec spec, boolean nullable, int precision) {
        this.spec = spec;
        this.nullable = nullable;
        this.precision = precision;

        computeDigest();
    }

    /** Returns the name of this type. A short handfor {@code spec().typeName() }. **/
    public final String getCustomTypeName() {
        return spec.typeName();
    }

    /**
     * Returns the {@link IgniteCustomTypeSpec specification} of this type.
     */
    public final IgniteCustomTypeSpec spec() {
        return spec;
    }

    /** {@inheritDoc} */
    @Override public final boolean isNullable() {
        return nullable;
    }

    /** {@inheritDoc} */
    @Override public RelDataTypeFamily getFamily() {
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
     * @return An SQL type name spec.
     */
    public final SqlTypeNameSpec createTypeNameSpec() {
        SqlIdentifier typeNameId = new SqlIdentifier(getCustomTypeName(), SqlParserPos.ZERO);

        if (getPrecision() == PRECISION_NOT_SPECIFIED) {
            return new IgniteSqlTypeNameSpec(typeNameId, SqlParserPos.ZERO);
        } else {
            SqlNumericLiteral precision = SqlLiteral.createExactNumeric(Integer.toString(getPrecision()), SqlParserPos.ZERO);

            return new IgniteSqlTypeNameSpec(typeNameId, precision, SqlParserPos.ZERO);
        }
    }
}
