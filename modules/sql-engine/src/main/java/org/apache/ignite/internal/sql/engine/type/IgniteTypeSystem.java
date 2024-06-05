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

import java.math.BigDecimal;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;

/**
 * Ignite type system.
 */
public class IgniteTypeSystem extends RelDataTypeSystemImpl {
    public static final IgniteTypeSystem INSTANCE = new IgniteTypeSystem();

    /** {@inheritDoc} */
    @Override
    public int getMaxNumericScale() {
        return CatalogUtils.MAX_DECIMAL_SCALE;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxNumericPrecision() {
        return CatalogUtils.MAX_DECIMAL_PRECISION;
    }

    /** {@inheritDoc} */
    @Override
    public int getMaxPrecision(SqlTypeName typeName) {
        switch (typeName) {
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return CatalogUtils.MAX_TIME_PRECISION;
            default:
                return super.getMaxPrecision(typeName);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int getDefaultPrecision(SqlTypeName typeName) {
        switch (typeName) {
            case TIMESTAMP: // DATETIME
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE: // TIMESTAMP
                // SQL`16 part 2 section 6.1 syntax rule 36
                return 6;
            case FLOAT:
                // Although FLOAT is an alias for REAL, we cannot use the same precision for them, w/o making
                // results of TypeFactory::LeastRestrictiveType() non-deterministic. 
                // We need to change the FLOAT precision, because by default calcite uses the precision for FLOAT and DOUBLE.
                // Assigning FLOAT a precision that is less than DOUBLE's works because:
                // - LeastRestrictiveType between REAL and FLOAT is FLOAT - OK, but both types are the same type.
                // - LeastRestrictiveType between FLOAT and DOUBLE is DOUBLE - OK, since FLOAT is an alias for REAL, and DOUBLE
                // represents a wider range of values.
                return super.getDefaultPrecision(typeName) - 1;
            default:
                return super.getDefaultPrecision(typeName);
        }
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType deriveSumType(RelDataTypeFactory typeFactory, RelDataType argumentType) {
        RelDataType sumType;
        // This method is used to derive types for aggregate functions only.
        // We allow type widening for aggregates to prevent unwanted number overflow.
        //
        // SQL`99 part 2 section 9.3 syntax rule 3:
        // Shortly, the standard says both, the return type and the argument type, must be of the same kind,
        // but doesn't restrict them being exactly of the same type.
        if (argumentType instanceof BasicSqlType) {
            switch (argumentType.getSqlTypeName()) {
                case INTEGER:
                case TINYINT:
                case SMALLINT:
                    sumType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT),
                            argumentType.isNullable());

                    break;

                case BIGINT:
                case DECIMAL:
                    sumType = typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(
                                    SqlTypeName.DECIMAL,
                                    typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.DECIMAL),
                                    argumentType.getScale()
                            ), argumentType.isNullable());

                    break;

                case REAL:
                case FLOAT:
                case DOUBLE:
                    sumType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE),
                            argumentType.isNullable());

                    break;

                default:
                    return super.deriveSumType(typeFactory, argumentType);
            }
        } else {
            switch (argumentType.getSqlTypeName()) {
                case INTEGER:
                case TINYINT:
                case SMALLINT:
                    sumType = typeFactory.createJavaType(Long.class);

                    break;

                case BIGINT:
                case DECIMAL:
                    sumType = typeFactory.createJavaType(BigDecimal.class);

                    break;

                case REAL:
                case FLOAT:
                case DOUBLE:
                    sumType = typeFactory.createJavaType(Double.class);

                    break;

                default:
                    return super.deriveSumType(typeFactory, argumentType);
            }
        }

        return typeFactory.createTypeWithNullability(sumType, argumentType.isNullable());
    }
}
