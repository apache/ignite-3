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

import static org.apache.calcite.rel.type.RelDataType.PRECISION_NOT_SPECIFIED;
import static org.apache.calcite.rel.type.RelDataType.SCALE_NOT_SPECIFIED;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_VARLEN_LENGTH;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.typeFamiliesAreCompatible;
import static org.apache.ignite.internal.util.CollectionUtils.first;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.ignite.internal.sql.engine.util.IgniteCustomAssignmentsRules;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.NativeTypes.RowTypeBuilder;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite type factory.
 */
public class IgniteTypeFactory extends JavaTypeFactoryImpl {
    /** Interval qualifier to create year-month interval types. */
    private static final SqlIntervalQualifier INTERVAL_QUALIFIER_YEAR_MONTH = new SqlIntervalQualifier(TimeUnit.YEAR,
            TimeUnit.MONTH, SqlParserPos.ZERO);

    /** Interval qualifier to create day-time interval types. */
    private static final SqlIntervalQualifier INTERVAL_QUALIFIER_DAY_TIME = new SqlIntervalQualifier(TimeUnit.DAY,
            TimeUnit.SECOND, SqlParserPos.ZERO);

    public static final IgniteTypeFactory INSTANCE = new IgniteTypeFactory(IgniteTypeSystem.INSTANCE);

    /** Contains java types internally mapped into appropriate rel types. */
    private final Map<Class<?>, Supplier<RelDataType>> implementedJavaTypes = new IdentityHashMap<>();

    /** Default charset. */
    private final Charset charset;

    {
        implementedJavaTypes.put(LocalDate.class, () ->
                createTypeWithNullability(createSqlType(SqlTypeName.DATE), true));
        implementedJavaTypes.put(LocalTime.class, () ->
                createTypeWithNullability(createSqlType(SqlTypeName.TIME), true));
        implementedJavaTypes.put(LocalDateTime.class, () ->
                createTypeWithNullability(createSqlType(SqlTypeName.TIMESTAMP), true));
        implementedJavaTypes.put(Instant.class, () ->
                createTypeWithNullability(createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE), true));
        implementedJavaTypes.put(Duration.class, () ->
                createTypeWithNullability(createSqlIntervalType(INTERVAL_QUALIFIER_DAY_TIME), true));
        implementedJavaTypes.put(Period.class, () ->
                createTypeWithNullability(createSqlIntervalType(INTERVAL_QUALIFIER_YEAR_MONTH), true));
    }

    /**
     * Constructor.
     *
     * @param typeSystem Type system.
     */
    public IgniteTypeFactory(RelDataTypeSystem typeSystem) {
        super(typeSystem);

        if (SqlUtil.translateCharacterSetName(Charset.defaultCharset().name()) != null) {
            // Use JVM default charset rather then Calcite default charset (ISO-8859-1).
            charset = Charset.defaultCharset();
        } else {
            // If JVM default charset is not supported by Calcite - use UTF-8.
            charset = StandardCharsets.UTF_8;
        }
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType createSqlType(SqlTypeName typeName, int precision) {
        // Default implementation converts precision > maxPrecision to maxPrecision
        assertBasicType(typeName);

        if (typeName.allowsScale()) {
            return createSqlType(typeName, precision, typeSystem.getDefaultScale(typeName));
        }

        assert (precision >= 0) || (precision == PRECISION_NOT_SPECIFIED);

        // Does not check precision when typeName is SqlTypeName#NULL.
        RelDataType newType = precision == PRECISION_NOT_SPECIFIED
                ? new BasicSqlType(typeSystem, typeName)
                : new BasicSqlType(typeSystem, typeName, precision);
        newType = SqlTypeUtil.addCharsetAndCollation(newType, this);
        return canonize(newType);
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType createSqlType(SqlTypeName typeName, int precision, int scale) {
        // Default implementation converts precision > maxPrecision to maxPrecision

        assertBasicType(typeName);

        assert (precision >= 0) || (precision == PRECISION_NOT_SPECIFIED);
        assert (scale >= 0) || (scale == SCALE_NOT_SPECIFIED);

        RelDataType newType = new BasicSqlType(typeSystem, typeName, precision, scale);
        newType = SqlTypeUtil.addCharsetAndCollation(newType, this);
        return canonize(newType);
    }

    private static void assertBasicType(SqlTypeName typeName) {
        assert typeName != null;
        assert typeName != SqlTypeName.MULTISET
                : "use createMultisetType() instead";
        assert typeName != SqlTypeName.ARRAY
                : "use createArrayType() instead";
        assert typeName != SqlTypeName.MAP
                : "use createMapType() instead";
        assert typeName != SqlTypeName.ROW
                : "use createStructType() instead";
        assert !SqlTypeName.INTERVAL_TYPES.contains(typeName)
                : "use createSqlIntervalType() instead";
    }

    /** {@inheritDoc} */
    @Override
    public Type getJavaClass(RelDataType type) {
        if (type instanceof JavaType) {
            return ((JavaType) type).getJavaClass();
        } else if (type instanceof BasicSqlType || type instanceof IntervalSqlType) {
            switch (type.getSqlTypeName()) {
                case VARCHAR:
                case CHAR:
                    return String.class;
                case DATE:
                case TIME:
                case TIME_WITH_LOCAL_TIME_ZONE:
                case INTEGER:
                case INTERVAL_YEAR:
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_MONTH:
                    return type.isNullable() ? Integer.class : int.class;
                case TIMESTAMP:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case BIGINT:
                case INTERVAL_DAY:
                case INTERVAL_DAY_HOUR:
                case INTERVAL_DAY_MINUTE:
                case INTERVAL_DAY_SECOND:
                case INTERVAL_HOUR:
                case INTERVAL_HOUR_MINUTE:
                case INTERVAL_HOUR_SECOND:
                case INTERVAL_MINUTE:
                case INTERVAL_MINUTE_SECOND:
                case INTERVAL_SECOND:
                    return type.isNullable() ? Long.class : long.class;
                case SMALLINT:
                    return type.isNullable() ? Short.class : short.class;
                case TINYINT:
                    return type.isNullable() ? Byte.class : byte.class;
                case DECIMAL:
                    return BigDecimal.class;
                case BOOLEAN:
                    return type.isNullable() ? Boolean.class : boolean.class;
                case DOUBLE:
                    return type.isNullable() ? Double.class : double.class;
                case REAL:
                case FLOAT:
                    return type.isNullable() ? Float.class : float.class;
                case BINARY:
                case VARBINARY:
                    return ByteString.class;
                case GEOMETRY:
                    throw new IllegalArgumentException("Type is not supported.");
                case SYMBOL:
                    return Enum.class;
                case ANY:
                case OTHER:
                    return Object.class;
                case NULL:
                    return Void.class;
                case UUID:
                    return UUID.class;
                default:
                    break;
            }
        }
        switch (type.getSqlTypeName()) {
            case ROW:
                return Object[].class; // At now
            case MAP:
                return Map.class;
            case ARRAY:
            case MULTISET:
                return List.class;
            default:
                return null;
        }
    }

    /**
     * Converts given {@link RelDataType relation type} to {@link NativeType native type}.
     *
     * @param relType Rel type.
     * @return Native type. Never null.
     * @throws IllegalArgumentException If conversion is not supported for given relation type.
     */
    public static NativeType relDataTypeToNative(RelDataType relType) throws IllegalArgumentException {
        assert relType instanceof BasicSqlType
                || relType instanceof IntervalSqlType
                || relType instanceof RelRecordType
                : "Not supported: " + relType;

        if (relType instanceof RelRecordType) {
            RowTypeBuilder builder = NativeTypes.rowBuilder();

            for (RelDataTypeField field : relType.getFieldList()) {
                builder.addField(field.getName(), relDataTypeToNative(field.getType()), field.getType().isNullable());
            }

            return builder.build(); 
        }

        switch (relType.getSqlTypeName()) {
            case NULL:
                return NativeTypes.NULL;
            case BOOLEAN:
                return NativeTypes.BOOLEAN;
            case TINYINT:
                return NativeTypes.INT8;
            case SMALLINT:
                return NativeTypes.INT16;
            case INTEGER:
                return NativeTypes.INT32;
            case BIGINT:
                return NativeTypes.INT64;
            case DECIMAL:
                assert relType.getPrecision() != PRECISION_NOT_SPECIFIED;

                return NativeTypes.decimalOf(relType.getPrecision(), relType.getScale());
            case FLOAT:
            case REAL:
                return NativeTypes.FLOAT;
            case DOUBLE:
                return NativeTypes.DOUBLE;
            case DATE:
                return NativeTypes.DATE;
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                return NativeTypes.time(precisionOrDefault(relType));
            case TIMESTAMP:
                return NativeTypes.datetime(precisionOrDefault(relType));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return NativeTypes.timestamp(precisionOrDefault(relType));
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
                // TODO: https://issues.apache.org/jira/browse/IGNITE-17373
                return NativeTypes.PERIOD;
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                // TODO: https://issues.apache.org/jira/browse/IGNITE-17373
                return NativeTypes.DURATION;
            case VARCHAR:
            case CHAR:
                return relType.getPrecision() == PRECISION_NOT_SPECIFIED
                        ? NativeTypes.stringOf(DEFAULT_VARLEN_LENGTH)
                        : NativeTypes.stringOf(relType.getPrecision());
            case BINARY:
            case VARBINARY:
                return relType.getPrecision() == PRECISION_NOT_SPECIFIED
                        ? NativeTypes.blobOf(DEFAULT_VARLEN_LENGTH)
                        : NativeTypes.blobOf(relType.getPrecision());
            case UUID:
                return NativeTypes.UUID;
            case ROW:
                RowTypeBuilder builder = NativeTypes.rowBuilder();

                for (RelDataTypeField field : relType.getFieldList()) {
                    builder.addField(field.getName(), relDataTypeToNative(field.getType()), field.getType().isNullable());
                }

                return builder.build(); 
            case ANY:
            default:
                throw new IllegalArgumentException("Type is not supported: " + relType);
        }
    }

    private static int precisionOrDefault(RelDataType type) {
        if (type.getPrecision() == PRECISION_NOT_SPECIFIED) {
            return IgniteTypeSystem.INSTANCE.getDefaultPrecision(type.getSqlTypeName());
        }

        return type.getPrecision();
    }

    /**
     * Returns resulting class.
     *
     * @param type Field logical type.
     * @return Result type.
     */
    public Type getResultClass(RelDataType type) {
        if (type instanceof JavaType) {
            return ((JavaType) type).getJavaClass();
        } else if (type instanceof BasicSqlType || type instanceof IntervalSqlType) {
            switch (type.getSqlTypeName()) {
                case VARCHAR:
                case CHAR:
                    return String.class;
                case DATE:
                    return LocalDate.class;
                case TIME:
                case TIME_WITH_LOCAL_TIME_ZONE:
                    return LocalTime.class;
                case TIMESTAMP:
                    return LocalDateTime.class;
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    return Instant.class;
                case INTEGER:
                    return type.isNullable() ? Integer.class : int.class;
                case INTERVAL_YEAR:
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_MONTH:
                    return Period.class;
                case BIGINT:
                    return type.isNullable() ? Long.class : long.class;
                case INTERVAL_DAY:
                case INTERVAL_DAY_HOUR:
                case INTERVAL_DAY_MINUTE:
                case INTERVAL_DAY_SECOND:
                case INTERVAL_HOUR:
                case INTERVAL_HOUR_MINUTE:
                case INTERVAL_HOUR_SECOND:
                case INTERVAL_MINUTE:
                case INTERVAL_MINUTE_SECOND:
                case INTERVAL_SECOND:
                    return Duration.class;
                case SMALLINT:
                    return type.isNullable() ? Short.class : short.class;
                case TINYINT:
                    return type.isNullable() ? Byte.class : byte.class;
                case DECIMAL:
                    return BigDecimal.class;
                case BOOLEAN:
                    return type.isNullable() ? Boolean.class : boolean.class;
                case DOUBLE:
                    return type.isNullable() ? Double.class : double.class;
                case REAL:
                case FLOAT:
                    return type.isNullable() ? Float.class : float.class;
                case BINARY:
                case VARBINARY:
                    return byte[].class;
                case GEOMETRY:
                    throw new IllegalArgumentException("Type is not supported.");
                case SYMBOL:
                    return Enum.class;
                case ANY:
                case OTHER:
                    return Object.class;
                case NULL:
                    return Void.class;
                default:
                    break;
            }
        }
        switch (type.getSqlTypeName()) {
            case ROW:
                return Object[].class; // At now
            case MAP:
                return Map.class;
            case ARRAY:
            case MULTISET:
                return List.class;
            default:
                return null;
        }
    }

    private static int getPrecision(RelDataType dataType) {
        if (dataType.getPrecision() == PRECISION_NOT_SPECIFIED) {
            return IgniteTypeSystem.INSTANCE.getDefaultPrecision(dataType.getSqlTypeName());
        } else {
            return dataType.getPrecision();
        }
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable RelDataType leastRestrictive(List<RelDataType> types) {
        assert types != null;
        assert !types.isEmpty();

        if (types.size() == 1 || allEquals(types)) {
            return first(types);
        }

        boolean hasAnyType = false;
        boolean hasNullOrNullable = false;
        boolean hasUuidType = false;
        boolean hasBuiltInType = false;

        for (var type : types) {
            SqlTypeName sqlTypeName = type.getSqlTypeName();
            // NULL types should be ignored when we are trying to determine the least restrictive type.
            if (sqlTypeName == SqlTypeName.NULL) {
                hasNullOrNullable = true;
                continue;
            }

            if (type.isNullable()) {
                hasNullOrNullable = true;
            }

            if (sqlTypeName == SqlTypeName.ANY) {
                hasAnyType = true;
            } else {
                if (sqlTypeName == SqlTypeName.UUID) {
                    hasUuidType = true;
                }

                hasBuiltInType = true;
            }
        }

        // Calcite's implementation of TypeFactory cannot derive least restrictive type between UUID and NULL.
        if (hasUuidType) {
            // UUID doesn't have any other types in the family, therefore it's safe to assume if all types are compatible,
            // then there are either UUID or NULL types.
            if (!typeFamiliesAreCompatible(this, types)) {
                return null;
            }

            RelDataType returnType = createSqlType(SqlTypeName.UUID);
            if (hasNullOrNullable) {
                returnType = createTypeWithNullability(returnType, true);
            }

            return returnType;
        }

        RelDataType resultTimestampType = leastRestrictiveBetweenTimestampTypes(types, hasNullOrNullable);

        if (resultTimestampType != null) {
            return resultTimestampType;
        }

        RelDataType resultType = leastRestrictive(types, IgniteCustomAssignmentsRules.instance());

        if (resultType == null) {
            return null;
        }

        if (resultType.getSqlTypeName() != SqlTypeName.ANY) {
            // leastRestrictive defined by calcite returns least restrictive even among types of different families.
            // We need to trim such variants.
            if (!typeFamiliesAreCompatible(this, types)) {
                return null;
            }

            return resultType;
        }

        // leastRestrictive defined by calcite returns an instance of BasicSqlType that represents an ANY type,
        // when at least one of its arguments have sqlTypeName = ANY.
        assert resultType instanceof BasicSqlType : "leastRestrictive is expected to return a new instance of a type: " + resultType;

        if (hasAnyType && hasBuiltInType) {
            // When at least one of arguments have sqlTypeName = ANY,
            // return it in order to be consistent with default implementation.
            return resultType;
        } else {
            return null;
        }
    }

    private @Nullable RelDataType leastRestrictiveBetweenTimestampTypes(List<RelDataType> types, boolean hasNullOrNullable) {
        RelDataType firstType = null;
        // If some types are nullable, the result must be nullable as well
        boolean nullable = hasNullOrNullable;

        for (RelDataType t : types) {
            if (t.getSqlTypeName() == SqlTypeName.NULL) {
                continue;
            }

            if (!TypeUtils.isTimestamp(t.getSqlTypeName())) {
                return null;
            }

            if (firstType == null) {
                firstType = t;

                if (t.isNullable()) {
                    nullable = true;
                }
            } else {
                RelDataType leftType = firstType;
                RelDataType rightType = t;

                if (t.isNullable()) {
                    nullable = true;
                }

                // TIMESTAMP vs TIMESTAMP_LTZ -> TIMESTAMP
                if (leftType.getSqlTypeName() == SqlTypeName.TIMESTAMP
                        && rightType.getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {

                    int lp = getPrecision(leftType);
                    int rp = getPrecision(rightType);

                    if (lp != rp) {
                        firstType = createSqlType(leftType.getSqlTypeName(), Math.max(lp, rp));
                    }
                } else if (rightType.getSqlTypeName() == SqlTypeName.TIMESTAMP
                        && leftType.getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {

                    int lp = getPrecision(leftType);
                    int rp = getPrecision(rightType);

                    if (lp != rp) {
                        firstType = createSqlType(rightType.getSqlTypeName(), Math.max(lp, rp));
                    } else {
                        firstType = rightType;
                    }
                } else {
                    int lp = getPrecision(leftType);
                    int rp = getPrecision(rightType);

                    if (rp > lp) {
                        firstType = rightType;
                    }
                }
            }
        }

        assert firstType != null;

        if (firstType.isNullable() != nullable) {
            return createTypeWithNullability(firstType, nullable);
        } else {
            return firstType;
        }
    }

    /** {@inheritDoc} */
    @Override
    public Charset getDefaultCharset() {
        return charset;
    }

    /** {@inheritDoc} */
    @Override public RelDataType toSql(RelDataType type) {
        if (type instanceof JavaType) {
            Class<?> clazz = ((JavaType) type).getJavaClass();

            Supplier<RelDataType> javaType = implementedJavaTypes.get(clazz);

            if (javaType != null) {
                return javaType.get();
            }
        }

        return super.toSql(type);
    }

    /** {@inheritDoc} */
    @Override public RelDataType createType(Type type) {
        //noinspection SuspiciousMethodCalls
        if (implementedJavaTypes.containsKey(type)) {
            return createJavaType((Class<?>) type);
        } else {
            return super.createType(type);
        }
    }

    private static boolean allEquals(List<RelDataType> types) {
        assert types.size() > 1;

        RelDataType first = first(types);
        for (int i = 1; i < types.size(); i++) {
            if (!Objects.equals(first, types.get(i))) {
                return false;
            }
        }

        return true;
    }
}
