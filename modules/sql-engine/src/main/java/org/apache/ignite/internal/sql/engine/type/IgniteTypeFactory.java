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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.sql.engine.util.Commons;
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

    /** Default charset. */
    private final Charset charset;

    /** A registry that contains custom data types. **/
    private final CustomDataTypes customDataTypes;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public IgniteTypeFactory() {
        this(IgniteTypeSystem.INSTANCE);
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

        // IgniteCustomType: all prototypes of custom types are registered here.
        NewCustomType uuidType = new NewCustomType(UuidType.NAME, UuidType.JAVA_TYPE, (nullable, precision) -> new UuidType(nullable));
        customDataTypes = new CustomDataTypes(Set.of(uuidType));
    }

    /** {@inheritDoc} */
    @Override
    public Type getJavaClass(RelDataType type) {
        if (type instanceof JavaType) {
            return ((JavaType) type).getJavaClass();
        } else if (type instanceof BasicSqlType || type instanceof IntervalSqlType || type instanceof IgniteCustomType) {
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
                    if (type instanceof IgniteCustomType) {
                        var customType = (IgniteCustomType) type;
                        return customType.storageType();
                    }
                    // fallthrough
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

    /**
     * Gets ColumnType type for RelDataType.
     *
     * @param relType Rel type.
     * @return ColumnType type or null.
     */
    public static NativeType relDataTypeToNative(RelDataType relType) {
        assert relType instanceof BasicSqlType
                || relType instanceof IntervalSqlType
                || relType instanceof IgniteCustomType : "Not supported:" + relType;

        switch (relType.getSqlTypeName()) {
            case BOOLEAN:
                //TODO: https://issues.apache.org/jira/browse/IGNITE-17298
                throw new IllegalArgumentException("Type is not supported yet: " + relType);
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
                return relType.getPrecision() == PRECISION_NOT_SPECIFIED ? NativeTypes.time() :
                        NativeTypes.time(relType.getPrecision());
            case TIMESTAMP:
                return relType.getPrecision() == PRECISION_NOT_SPECIFIED ? NativeTypes.datetime() :
                        NativeTypes.datetime(relType.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return relType.getPrecision() == PRECISION_NOT_SPECIFIED ? NativeTypes.timestamp() :
                        NativeTypes.timestamp(relType.getPrecision());
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
                //TODO: https://issues.apache.org/jira/browse/IGNITE-17373
                throw new IllegalArgumentException("Type is not supported yet: " + relType);
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
                //TODO: https://issues.apache.org/jira/browse/IGNITE-17373
                throw new IllegalArgumentException("Type is not supported yet:" + relType);
            case VARCHAR:
            case CHAR:
                return relType.getPrecision() == PRECISION_NOT_SPECIFIED
                        ? NativeTypes.stringOf(Integer.MAX_VALUE)
                        : NativeTypes.stringOf(relType.getPrecision());
            case BINARY:
            case VARBINARY:
                return relType.getPrecision() == PRECISION_NOT_SPECIFIED
                        ? NativeTypes.blobOf(Integer.MAX_VALUE)
                        : NativeTypes.blobOf(relType.getPrecision());
            case ANY:
                if (relType instanceof IgniteCustomType) {
                    var customType = (IgniteCustomType) relType;
                    return customType.nativeType();
                }
                // fallthrough
            default:
                throw new IllegalArgumentException("Type is not supported: " + relType);
        }
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
        } else if (type instanceof BasicSqlType || type instanceof IntervalSqlType || type instanceof IgniteCustomType) {
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
                    if (type instanceof IgniteCustomType) {
                        var customType = (IgniteCustomType) type;
                        var nativeType = customType.nativeType();
                        return Commons.nativeTypeToClass(nativeType);
                    }
                    // fallthrough
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

    /** {@inheritDoc} */
    @Override
    public @Nullable RelDataType leastRestrictive(List<RelDataType> types) {
        assert types != null;
        assert types.size() >= 1;

        if (types.size() == 1 || allEquals(types)) {
            return first(types);
        }

        RelDataType resultType = super.leastRestrictive(types);

        if (resultType != null && resultType.getSqlTypeName() == SqlTypeName.ANY) {
            // leastRestrictive defined by calcite returns an instance of BasicSqlType that represents an ANY type,
            // when at least one of its arguments have sqlTypeName = ANY.
            assert resultType instanceof BasicSqlType : "leastRestrictive is expected to return a new instance of a type: " + resultType;

            IgniteCustomType firstCustomType = null;
            SqlTypeFamily sqlTypeFamily = null;

            for (var type : types) {
                if (type instanceof IgniteCustomType) {
                    var customType = (IgniteCustomType) type;

                    if (firstCustomType == null) {
                        firstCustomType = (IgniteCustomType) type;
                    } else if (!Objects.equals(firstCustomType.getCustomTypeName(), customType.getCustomTypeName())) {
                        // IgniteCustomType: Conversion between custom data types is not supported.
                        return null;
                    }
                } else if (SqlTypeUtil.isCharacter(type)) {
                    sqlTypeFamily = type.getSqlTypeName().getFamily();
                }
            }

            if (firstCustomType != null && sqlTypeFamily != null) {
                // IgniteCustomType: we allow implicit casts from VARCHAR to custom data types.
                return firstCustomType;
            } else {
                return resultType;
            }
        } else {
            return resultType;
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

            // why do we make types nullable here?
            if (clazz == Duration.class) {
                return createTypeWithNullability(createSqlIntervalType(INTERVAL_QUALIFIER_DAY_TIME), true);
            } else if (clazz == Period.class) {
                return createTypeWithNullability(createSqlIntervalType(INTERVAL_QUALIFIER_YEAR_MONTH), true);
            }
        }

        return super.toSql(type);
    }

    /** {@inheritDoc} **/
    @Override
    public RelDataType createTypeWithNullability(RelDataType type, boolean nullable) {
        if (type instanceof IgniteCustomType) {
            return canonize(((IgniteCustomType) type).createWithNullability(nullable));
        } else {
            return super.createTypeWithNullability(type, nullable);
        }
    }

    /** {@inheritDoc} */
    @Override public RelDataType createType(Type type) {
        if (type == Duration.class || type == Period.class || type == LocalDate.class || type == LocalDateTime.class
                || type == LocalTime.class) {
            return createJavaType((Class<?>) type);
        } else if (customDataTypes.javaTypes.contains(type)) {
            throw new IllegalArgumentException("Custom data type should not be created via createType call: " + type);
        } else {
            return super.createType(type);
        }
    }

    /** {@inheritDoc} **/
    @Override
    public RelDataType createJavaType(Class clazz) {
        if (customDataTypes.javaTypes.contains(clazz)) {
            throw new IllegalArgumentException("Custom data type should not be created via createJavaType call: " + clazz);
        } else {
            return super.createJavaType(clazz);
        }
    }

    /**
     * Creates a custom data type with the given {@code typeName} and precision.
     *
     * @param typeName type name.
     * @param precision precision if supported.
     * @return a custom data type.
     */
    public RelDataType createCustomType(String typeName, int precision) {
        IgniteCustomTypeFactory customTypeFactory = customDataTypes.typeConstructors.get(typeName);
        if (customTypeFactory == null) {
            throw new IllegalArgumentException("Unexpected custom data type: " + typeName);
        }

        // By default a type must not be nullable.
        // See SqlTypeFactory::createSqlType.
        //
        // TODO workaround for https://issues.apache.org/jira/browse/CALCITE-5297
        //  Set nullable to false and uncomment the assertion after upgrade to calcite 1.33.
        IgniteCustomType customType = customTypeFactory.newType(true, precision);
        // assert !customType.isNullable() : "makeCustomType must not return a nullable type: " + typeName + " " + customType;
        return canonize(customType);
    }

    /**
     * Creates a custom data type with the given {@code typeName} and without precision.
     * A shorthand for {@code createCustomType(typeName, -1)}.
     *
     * @param typeName type name.
     * @return a custom data type.
     */
    public RelDataType createCustomType(String typeName) {
        return createCustomType(typeName, PRECISION_NOT_SPECIFIED);
    }

    private boolean allEquals(List<RelDataType> types) {
        assert types.size() > 1;

        RelDataType first = first(types);
        for (int i = 1; i < types.size(); i++) {
            if (!Objects.equals(first, types.get(i))) {
                return false;
            }
        }

        return true;
    }

    private static final class CustomDataTypes {

        /**
         * Contains java types used registered custom data types.
         * We need those to throw errors to reject attempts to create custom data types via
         * {@link IgniteTypeFactory#createType(Type)}/{@link IgniteTypeFactory#createJavaType(Class)}
         * methods of {@link IgniteTypeFactory}.
         */
        private final Set<Type> javaTypes;

        /**
         * Stores functions that are being used by {@link #createCustomType(String, int)} to create type instances.
         */
        private final Map<String, IgniteCustomTypeFactory> typeConstructors;

        CustomDataTypes(Set<NewCustomType> customDataTypes) {
            this.javaTypes = customDataTypes.stream()
                    .map(t -> t.storageType)
                    .collect(Collectors.toSet());

            this.typeConstructors = customDataTypes.stream().collect(Collectors.toMap((v) -> v.typeName, (v) -> v.makeType));
        }
    }

    private static final class NewCustomType {
        final String typeName;

        final Class<?> storageType;

        final IgniteCustomTypeFactory makeType;

        NewCustomType(String typeName, Class<?> storageType, IgniteCustomTypeFactory makeType) {
            this.typeName = typeName;
            this.storageType = storageType;
            this.makeType = makeType;
        }
    }

    @FunctionalInterface
    interface IgniteCustomTypeFactory {
        IgniteCustomType newType(boolean nullable, int precision);
    }

    /** {@inheritDoc} */
    @Override public RelDataType createUnknownType() {
        // TODO workaround for https://issues.apache.org/jira/browse/CALCITE-5297
        // Remove this after update to Calcite 1.33.
        return createTypeWithNullability(super.createUnknownType(), true);
    }
}
