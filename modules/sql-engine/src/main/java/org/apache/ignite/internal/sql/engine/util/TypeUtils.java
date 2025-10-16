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

package org.apache.ignite.internal.sql.engine.util;

import static org.apache.calcite.sql.type.SqlTypeName.BINARY_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.STRING_TYPES;
import static org.apache.ignite.internal.sql.engine.util.IgniteMath.convertToIntExact;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeName.Limit;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.ignite.internal.sql.engine.SchemaAwareConverter;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowBuilder;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.row.BaseTypeSpec;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchemaTypes;
import org.apache.ignite.internal.sql.engine.exec.row.RowType;
import org.apache.ignite.internal.sql.engine.exec.row.TypeSpec;
import org.apache.ignite.internal.sql.engine.prepare.ParameterType;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.TemporalNativeType;
import org.apache.ignite.internal.type.VarlenNativeType;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * TypeUtils.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class TypeUtils {
    public static final SchemaAwareConverter<Object, Object> IDENTITY_ROW_CONVERTER = (idx, r) -> r;

    private static final Set<SqlTypeName> CONVERTABLE_TYPES = EnumSet.of(
            SqlTypeName.DATE,
            SqlTypeName.TIME,
            SqlTypeName.BINARY,
            SqlTypeName.VARBINARY,
            SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE,
            SqlTypeName.TIMESTAMP,
            SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
            SqlTypeName.INTERVAL_SECOND,
            SqlTypeName.INTERVAL_MINUTE,
            SqlTypeName.INTERVAL_MINUTE_SECOND,
            SqlTypeName.INTERVAL_HOUR,
            SqlTypeName.INTERVAL_HOUR_MINUTE,
            SqlTypeName.INTERVAL_HOUR_SECOND,
            SqlTypeName.INTERVAL_DAY,
            SqlTypeName.INTERVAL_DAY_HOUR,
            SqlTypeName.INTERVAL_DAY_MINUTE,
            SqlTypeName.INTERVAL_DAY_SECOND,
            SqlTypeName.INTERVAL_MONTH,
            SqlTypeName.INTERVAL_YEAR,
            SqlTypeName.INTERVAL_YEAR_MONTH
    );

    /**
     * Returns the upper bound value for the given SQL type as a {@link BigDecimal}, if applicable.
     *
     * <p>If the type is not a numeric type, this method returns {@code null}.</p>
     *
     * @param type The {@link RelDataType} representing the SQL type
     * @return A {@link BigDecimal} representing the maximum value for the given type, or {@code null} if the type is not a numeric.
     */
    public static @Nullable BigDecimal upperBoundFor(RelDataType type) {
        switch (type.getSqlTypeName()) {
            case TINYINT: return BigDecimal.valueOf(Byte.MAX_VALUE);
            case SMALLINT: return BigDecimal.valueOf(Short.MAX_VALUE);
            case INTEGER: return BigDecimal.valueOf(Integer.MAX_VALUE);
            case BIGINT: return BigDecimal.valueOf(Long.MAX_VALUE);
            case REAL: return BigDecimal.valueOf(Float.MAX_VALUE);
            case DOUBLE: return BigDecimal.valueOf(Double.MAX_VALUE);
            case DECIMAL: return (BigDecimal) type.getSqlTypeName()
                    .getLimit(true, Limit.OVERFLOW, false, type.getPrecision(), type.getScale());
            default: return null;
        }
    }

    /**
     * Returns the lower bound value for the given SQL type as a {@link BigDecimal}, if applicable.
     *
     * <p>If the type is not a numeric type, this method returns {@code null}.</p>
     *
     * @param type The {@link RelDataType} representing the SQL type
     * @return A {@link BigDecimal} representing the minimum value for the given type, or {@code null} if the type is not a numeric.
     */
    public static @Nullable BigDecimal lowerBoundFor(RelDataType type) {
        switch (type.getSqlTypeName()) {
            case TINYINT: return BigDecimal.valueOf(Byte.MIN_VALUE);
            case SMALLINT: return BigDecimal.valueOf(Short.MIN_VALUE);
            case INTEGER: return BigDecimal.valueOf(Integer.MIN_VALUE);
            case BIGINT: return BigDecimal.valueOf(Long.MIN_VALUE);
            case REAL: return BigDecimal.valueOf(-Float.MAX_VALUE);
            case DOUBLE: return BigDecimal.valueOf(-Double.MAX_VALUE);
            case DECIMAL: return (BigDecimal) type.getSqlTypeName()
                    .getLimit(false, Limit.OVERFLOW, false, type.getPrecision(), type.getScale());
            default: return null;
        }
    }

    /** Creates parameter metadata from the given logical type. */
    public static ParameterType fromRelDataType(RelDataType type) {
        ColumnType columnType = columnType(type);
        assert columnType != null : "No column type for " + type;

        int precision = columnType.lengthAllowed() || columnType.precisionAllowed()
                ? type.getPrecision()
                : ColumnMetadata.UNDEFINED_PRECISION;

        int scale = columnType.scaleAllowed() ? type.getScale() : ColumnMetadata.UNDEFINED_SCALE;

        return new ParameterType(columnType, precision, scale, type.isNullable());
    }

    private static class SupportedParamClassesHolder {
        // TODO: https://issues.apache.org/jira/browse/IGNITE-17373
        static final Set<ColumnType> UNSUPPORTED_COLUMN_TYPES_AS_PARAMETERS = Set.of(ColumnType.PERIOD, ColumnType.DURATION);
        static final Set<Class<?>> SUPPORTED_PARAM_CLASSES;

        static {
            SUPPORTED_PARAM_CLASSES = Arrays.stream(ColumnType.values())
                    .filter(t -> !UNSUPPORTED_COLUMN_TYPES_AS_PARAMETERS.contains(t))
                    .map(ColumnType::javaClass).collect(Collectors.toUnmodifiableSet());
        }
    }

    private static Set<Class<?>> supportedParamClasses() {
        return SupportedParamClassesHolder.SUPPORTED_PARAM_CLASSES;
    }

    /** Return {@code true} if supplied object is suitable as dynamic parameter. */
    public static boolean supportParamInstance(@Nullable Object param) {
        return param == null || supportedParamClasses().contains(param.getClass());
    }

    /**
     * CombinedRowType.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static RelDataType combinedRowType(IgniteTypeFactory typeFactory, RelDataType... types) {

        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);

        Set<String> names = new HashSet<>();

        for (RelDataType type : types) {
            for (RelDataTypeField field : type.getFieldList()) {
                int idx = 0;
                String fieldName = field.getName();

                while (!names.add(fieldName)) {
                    fieldName = field.getName() + idx++;
                }

                builder.add(fieldName, field.getType());
            }
        }

        return builder.build();
    }

    /** Assembly output type from input types. */
    public static RelDataType createRowType(IgniteTypeFactory typeFactory, List<RelDataType> fields) {
        return createRowType(typeFactory, fields, "$F");
    }

    private static RelDataType createRowType(IgniteTypeFactory typeFactory, List<RelDataType> fields, String namePreffix) {
        List<String> names = IntStream.range(0, fields.size())
                .mapToObj(ord -> namePreffix + ord)
                .collect(Collectors.toList());

        return typeFactory.createStructType(fields, names);
    }

    /**
     * Provide a function to convert internal representation of sql results into external types.
     *
     * @param resultType Type of result.
     * @return Schema-aware converting function.
     */
    public static SchemaAwareConverter<Object, Object> resultTypeConverter(RelDataType resultType) {
        assert resultType.isStruct();

        if (hasConvertableFields(resultType)) {
            List<RelDataType> types = RelOptUtil.getFieldTypeList(resultType);
            Function<Object, Object>[] converters = (Function<Object, Object>[]) new Function[types.size()];
            for (int i = 0; i < types.size(); i++) {
                converters[i] = fieldConverter(types.get(i));
            }

            return (idx, r) -> {
                assert idx >= 0 && idx < converters.length;
                return converters[idx].apply(r);
            };
        }

        return IDENTITY_ROW_CONVERTER;
    }

    private static Function<@Nullable Object, @Nullable Object> fieldConverter(RelDataType fieldType) {
        if (isConvertableType(fieldType)) {
            ColumnType storageType = columnType(fieldType);

            return v -> v == null ? null : fromInternal(v, storageType);
        }

        return Function.identity();
    }

    /**
     * IsConvertableType.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static boolean isConvertableType(RelDataType type) {
        return CONVERTABLE_TYPES.contains(type.getSqlTypeName());
    }

    private static boolean hasConvertableFields(RelDataType resultType) {
        for (RelDataTypeField field : resultType.getFieldList()) {
            if (isConvertableType(field.getType())) {
                return true;
            }
        }

        return false;
    }

    /**
     * Converts the given value to its presentation used by the execution engine.
     */
    public static Object toInternal(Object val, ColumnType spec) {
        switch (spec) {
            case INT8: {
                assert val instanceof Byte : val.getClass();
                return val;
            }
            case INT16: {
                assert val instanceof Short : val.getClass();
                return val;
            }
            case INT32: {
                assert val instanceof Integer : val.getClass();
                return val;
            }
            case INT64: {
                assert val instanceof Long : val.getClass();
                return val;
            }
            case FLOAT: {
                assert val instanceof Float : val.getClass();
                return val;
            }
            case DOUBLE: {
                assert val instanceof Double : val.getClass();
                return val;
            }
            case DECIMAL: {
                assert val instanceof BigDecimal : val.getClass();
                return val;
            }
            case UUID: {
                assert val instanceof UUID : val.getClass();
                return val;
            }
            case STRING: {
                assert val instanceof String : val.getClass();
                return val;
            }
            case BYTE_ARRAY: {
                if (val instanceof String) {
                    return new ByteString(((String) val).getBytes(StandardCharsets.UTF_8));
                } else if (val instanceof byte[]) {
                    return new ByteString((byte[]) val);
                } else {
                    assert val instanceof ByteString : val.getClass();
                    return val;
                }
            }
            case DATE: {
                assert val instanceof LocalDate : val.getClass();
                return (int) ((LocalDate) val).toEpochDay();
            }
            case TIME: {
                assert val instanceof LocalTime : val.getClass();
                return (int) (TimeUnit.NANOSECONDS.toMillis(((LocalTime) val).toNanoOfDay()));
            }
            case DATETIME: {
                assert val instanceof LocalDateTime : val.getClass();
                var dt = (LocalDateTime) val;
                return TimeUnit.SECONDS.toMillis(dt.toEpochSecond(ZoneOffset.UTC)) + TimeUnit.NANOSECONDS.toMillis(dt.getNano());
            }
            case TIMESTAMP: {
                assert val instanceof Instant : val.getClass();
                return ((Instant) val).toEpochMilli();
            }
            case BOOLEAN:
                assert val instanceof Boolean : val.getClass();
                return val;
            case DURATION:
                return ((Duration) val).toMillis();
            case PERIOD:
                return convertToIntExact(((Period) val).toTotalMonths());

            default: {
                throw new AssertionError("Type is not supported: " + spec);
            }
        }
    }

    /**
     * Converts the value from its presentation used by the execution engine.
     */
    public static Object fromInternal(Object val, ColumnType spec) {
        switch (spec) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case UUID:
            case STRING:
            case BOOLEAN:
                return val;
            case BYTE_ARRAY:
                return ((ByteString) val).getBytes();
            case DATE:
                return LocalDate.ofEpochDay((Integer) val);
            case TIME:
                return LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(Long.valueOf((Integer) val)));
            case DATETIME:
                return LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) val), ZoneOffset.UTC);
            case TIMESTAMP:
                return Instant.ofEpochMilli((Long) val);
            case DURATION: {
                assert val instanceof Long;
                return Duration.ofMillis((Long) val);
            }
            case PERIOD: {
                assert val instanceof Integer;
                return Period.of((Integer) val / 12, (Integer) val % 12, 0);
            }
            default: {
                throw new AssertionError("Type is not supported: " + spec);
            }
        }
    }

    /**
     * Convert calcite date type to Ignite native type.
     */
    public static ColumnType columnType(RelDataType type) {
        switch (type.getSqlTypeName()) {
            case VARCHAR:
            case CHAR:
                return ColumnType.STRING;
            case DATE:
                return ColumnType.DATE;
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                return ColumnType.TIME;
            case INTEGER:
                return ColumnType.INT32;
            case TIMESTAMP:
                return ColumnType.DATETIME;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return ColumnType.TIMESTAMP;
            case BIGINT:
                return ColumnType.INT64;
            case SMALLINT:
                return ColumnType.INT16;
            case TINYINT:
                return ColumnType.INT8;
            case BOOLEAN:
                return ColumnType.BOOLEAN;
            case DECIMAL:
                return ColumnType.DECIMAL;
            case DOUBLE:
                return ColumnType.DOUBLE;
            case REAL:
            case FLOAT:
                return ColumnType.FLOAT;
            case BINARY:
            case VARBINARY:
            case ANY:
            case OTHER:
                return ColumnType.BYTE_ARRAY;
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
                return ColumnType.PERIOD;
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
            case INTERVAL_DAY:
                return ColumnType.DURATION;
            case NULL:
                return ColumnType.NULL;
            case UUID:
                return ColumnType.UUID;
            default:
                throw new IllegalArgumentException("Unexpected type: " + type.getSqlTypeName());
        }
    }

    /**
     * Converts a {@link NativeType native type} to {@link RelDataType relational type}.
     *
     * @param factory Type factory.
     * @param nativeType A native type to convert.
     * @return Relational type.
     */
    public static RelDataType native2relationalType(RelDataTypeFactory factory, NativeType nativeType) {
        switch (nativeType.spec()) {
            case BOOLEAN:
                return factory.createSqlType(SqlTypeName.BOOLEAN);
            case INT8:
                return factory.createSqlType(SqlTypeName.TINYINT);
            case INT16:
                return factory.createSqlType(SqlTypeName.SMALLINT);
            case INT32:
                return factory.createSqlType(SqlTypeName.INTEGER);
            case INT64:
                return factory.createSqlType(SqlTypeName.BIGINT);
            case FLOAT:
                return factory.createSqlType(SqlTypeName.REAL);
            case DOUBLE:
                return factory.createSqlType(SqlTypeName.DOUBLE);
            case DECIMAL:
                assert nativeType instanceof DecimalNativeType;

                var decimal = (DecimalNativeType) nativeType;

                return factory.createSqlType(SqlTypeName.DECIMAL, decimal.precision(), decimal.scale());
            case UUID:
                return factory.createSqlType(SqlTypeName.UUID);
            case STRING: {
                assert nativeType instanceof VarlenNativeType;

                var varlen = (VarlenNativeType) nativeType;

                return factory.createSqlType(SqlTypeName.VARCHAR, varlen.length());
            }
            case BYTE_ARRAY: {
                assert nativeType instanceof VarlenNativeType;

                var varlen = (VarlenNativeType) nativeType;

                return factory.createSqlType(SqlTypeName.VARBINARY, varlen.length());
            }
            case DATE:
                return factory.createSqlType(SqlTypeName.DATE);
            case TIME:
                assert nativeType instanceof TemporalNativeType;

                var time = (TemporalNativeType) nativeType;

                return factory.createSqlType(SqlTypeName.TIME, time.precision());
            case TIMESTAMP:
                assert nativeType instanceof TemporalNativeType;

                var ts = (TemporalNativeType) nativeType;

                return factory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, ts.precision());
            case DATETIME:
                assert nativeType instanceof TemporalNativeType;

                var dt = (TemporalNativeType) nativeType;

                return factory.createSqlType(SqlTypeName.TIMESTAMP, dt.precision());
            default:
                throw new IllegalStateException("Unexpected native type " + nativeType);
        }
    }

    /**
     * Converts a {@link NativeType native type} to {@link RelDataType relational type} with respect to the nullability flag.
     *
     * @param factory Type factory.
     * @param nativeType A native type to convert.
     * @param nullable A flag that specify whether the resulting type should be nullable or not.
     * @return Relational type.
     */
    public static RelDataType native2relationalType(RelDataTypeFactory factory, NativeType nativeType, boolean nullable) {
        return factory.createTypeWithNullability(native2relationalType(factory, nativeType), nullable);
    }

    /**
     * Converts a {@link NativeType native types} to {@link RelDataType relational types}.
     *
     * @param factory Type factory.
     * @param nativeTypes A native types to convert.
     * @return Relational types.
     */
    public static List<RelDataType> native2relationalTypes(RelDataTypeFactory factory, NativeType... nativeTypes) {
        return Arrays.stream(nativeTypes).map(t -> native2relationalType(factory, t)).collect(Collectors.toList());
    }

    /** Converts {@link ColumnType} to corresponding {@link NativeType}. */
    public static NativeType columnType2NativeType(ColumnType columnType, int precision, int scale, int length) {
        switch (columnType) {
            case BOOLEAN:
                return NativeTypes.BOOLEAN;
            case INT8:
                return NativeTypes.INT8;
            case INT16:
                return NativeTypes.INT16;
            case INT32:
                return NativeTypes.INT32;
            case INT64:
                return NativeTypes.INT64;
            case FLOAT:
                return NativeTypes.FLOAT;
            case DOUBLE:
                return NativeTypes.DOUBLE;
            case DECIMAL:
                return NativeTypes.decimalOf(precision, scale);
            case DATE:
                return NativeTypes.DATE;
            case TIME:
                return NativeTypes.time(precision);
            case DATETIME:
                return NativeTypes.datetime(precision);
            case TIMESTAMP:
                return NativeTypes.timestamp(precision);
            case UUID:
                return NativeTypes.UUID;
            case STRING:
                return NativeTypes.stringOf(length);
            case BYTE_ARRAY:
                return NativeTypes.blobOf(length);
            // fallthrough
            case PERIOD:
            case DURATION:
            case NULL:
            default:
                throw new IllegalArgumentException("No NativeType for type: " + columnType);
        }
    }

    /** Checks whether cast operation is necessary in {@code SearchBound}. */
    public static boolean needCastInSearchBounds(IgniteTypeFactory typeFactory, RelDataType fromType, RelDataType toType) {
        // Checks for character and binary types should allow comparison
        // between types with precision, types w/o precision, and varying non-varying length variants.
        // Otherwise the optimizer wouldn't pick an index for conditions such as
        // col (VARCHAR(M)) = CAST(s AS VARCHAR(N) (M != N) , col (VARCHAR) = CAST(s AS VARCHAR(N))

        // No need to cast between char and varchar.
        if (SqlTypeUtil.isCharacter(toType) && SqlTypeUtil.isCharacter(fromType)) {
            return false;
        }

        // No need to cast if the source type precedence list
        // contains target type. i.e. do not cast from
        // tinyint to int or int to bigint.
        if (fromType.getPrecedenceList().containsType(toType)
                && SqlTypeUtil.isIntType(fromType)
                && SqlTypeUtil.isIntType(toType)) {
            return false;
        }

        // TIME, TIMESTAMP and TIMESTAMP_WLTZ can use index, ignoring precision.
        if (fromType.getSqlTypeName() == toType.getSqlTypeName() && SqlTypeUtil.isDatetime(fromType)) {
            return false;
        }

        // Implicit type coercion does not handle nullability.
        if (SqlTypeUtil.equalSansNullability(typeFactory, fromType, toType)) {
            return false;
        }
        // Should keep sync with rules in SqlTypeCoercionRule.
        assert SqlTypeUtil.canCastFrom(toType, fromType, true);
        return true;
    }

    /**
     * Checks that {@code toType} and {@code fromType} have compatible type families taking into account custom data types. Types {@code T1}
     * and {@code T2} have compatible type families if {@code T1} can be assigned to {@code T2} and vice-versa.
     *
     * @see SqlTypeUtil#canAssignFrom(RelDataType, RelDataType)
     */
    public static boolean typeFamiliesAreCompatible(RelDataTypeFactory typeFactory, RelDataType toType, RelDataType fromType) {
        // Same types are always compatible.
        if (SqlTypeUtil.equalSansNullability(typeFactory, toType, fromType)) {
            return true;
        }

        // NULL is compatible with all types.
        if (fromType.getSqlTypeName() == SqlTypeName.NULL || toType.getSqlTypeName() == SqlTypeName.NULL) {
            return true;
        }

        if (toType.isStruct() && fromType.isStruct()) {
            if (toType.getFieldCount() != fromType.getFieldCount()) {
                return false;
            }

            for (int i = 0; i < toType.getFieldCount(); i++) {
                RelDataType type1 = toType.getFieldList().get(i).getType();
                RelDataType type2 = fromType.getFieldList().get(i).getType();

                if (!typeFamiliesAreCompatible(typeFactory, type1, type2)) {
                    return false;
                }
            }

            return true;
        }

        return SqlTypeUtil.canAssignFrom(toType, fromType)
                && SqlTypeUtil.canAssignFrom(fromType, toType);
    }

    /**
     * Checks that given types have compatible type families taking into account custom data types. Types {@code T1}
     * and {@code T2} have compatible type families if {@code T1} can be assigned to {@code T2} and vice-versa.
     *
     * @see SqlTypeUtil#canAssignFrom(RelDataType, RelDataType)
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public static boolean typeFamiliesAreCompatible(RelDataTypeFactory typeFactory, RelDataType... types) {
        return typeFamiliesAreCompatible(typeFactory, List.of(types));
    }

    /**
     * Checks that given types have compatible type families taking into account custom data types. Types {@code T1}
     * and {@code T2} have compatible type families if {@code T1} can be assigned to {@code T2} and vice-versa.
     *
     * @see SqlTypeUtil#canAssignFrom(RelDataType, RelDataType)
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public static boolean typeFamiliesAreCompatible(RelDataTypeFactory typeFactory, List<RelDataType> types) {
        if (types.size() < 2) {
            return true;
        }

        RelDataType firstType = null;
        for (RelDataType type : types) {
            if (firstType == null) {
                if (SqlTypeUtil.isNull(type)) {
                    // null is compatible with any other type, therefore we need to find
                    // first type that is not NULL to make check valid
                    continue;
                }

                firstType = type;
            } else if (!typeFamiliesAreCompatible(typeFactory, firstType, type)) {
                return false;
            }
        }

        return true;
    }

    /** Creates an instance of {@link RowSchema} from a list of the given {@link RelDataType}s. */
    public static RowSchema rowSchemaFromRelTypes(List<RelDataType> types) {
        RowSchema.Builder fieldTypes = RowSchema.builder();

        for (RelDataType relType : types) {
            TypeSpec typeSpec = convertToTypeSpec(relType);
            fieldTypes.addField(typeSpec);
        }

        return fieldTypes.build();
    }

    private static TypeSpec convertToTypeSpec(RelDataType type) {
        boolean simpleType = type instanceof BasicSqlType;
        boolean nullable = type.isNullable();

        if (SqlTypeName.ANY == type.getSqlTypeName()) {
            // TODO Some JSON functions that return ANY as well : https://issues.apache.org/jira/browse/IGNITE-20163
            return new BaseTypeSpec(null, nullable);
        } else if (SqlTypeUtil.isNull(type)) {
            return RowSchemaTypes.NULL;
        } else if (simpleType) {
            NativeType nativeType = IgniteTypeFactory.relDataTypeToNative(type);
            return RowSchemaTypes.nativeTypeWithNullability(nativeType, nullable);
        } else if (type instanceof IntervalSqlType) {
            IntervalSqlType intervalType = (IntervalSqlType) type;
            boolean yearMonth = intervalType.getIntervalQualifier().isYearMonth();

            if (yearMonth) {
                // YEAR MONTH interval is stored as number of days in ints.
                return RowSchemaTypes.nativeTypeWithNullability(NativeTypes.PERIOD, nullable);
            } else {
                // DAY interval is stored as time as long.
                return RowSchemaTypes.nativeTypeWithNullability(NativeTypes.DURATION, nullable);
            }
        } else if (SqlTypeUtil.isRow(type)) {
            List<TypeSpec> fields = new ArrayList<>();

            for (RelDataTypeField field : type.getFieldList()) {
                TypeSpec fieldTypeSpec = convertToTypeSpec(field.getType());
                fields.add(fieldTypeSpec);
            }

            return new RowType(fields, type.isNullable());

        } else if (SqlTypeUtil.isMap(type) || SqlTypeUtil.isMultiset(type) || SqlTypeUtil.isArray(type)) {
            // TODO https://issues.apache.org/jira/browse/IGNITE-20162
            //  Add collection types support
            throw new IllegalArgumentException("Collection types is not supported: " + type);
        } else {
            throw new IllegalArgumentException("Unexpected type: " + type);
        }
    }

    /** Check limitation for character and binary types and throws exception if row does not fit into type defined.
     *  <br>
     *  Store assignment section defines:
     *  If the declared type of T is fixed-length character string with length in characters L and
     *   the length in characters M of V is larger than L, then:
     *  <br>
     *  1) If the rightmost M-L characters of V are all space(s), then the value of T is set to
     *   the first L characters of V.
     *  <br>
     *  2) If one or more of the rightmost M-L characters of V are not space(s), then an
     *   exception condition is raised: data exception — string data, right truncation.
     *  <br><br>
     *  If the declared type of T is binary string and the length in octets M of V is greater than
     *   the maximum length in octets L of T, then:
     *  <br>
     *  1) If the rightmost M-L octets of V are all equal to X’00’, then the value of T is set to
     *   the first L octets of V and the length in octets of T is set to L.
     *  <br>
     *  2) If one or more of the rightmost M-L octets of V are not equal to X’00’, then an
     *   exception condition is raised: data exception — string data, right truncation.
     */
    public static <RowT> RowT validateStringTypesOverflowAndTrimIfPossible(
            RelDataType rowType,
            RowHandler<RowT> rowHandler,
            RowT row,
            Supplier<RowSchema> schema
    ) {
        boolean containValidatedType =
                rowType.getFieldList().stream().anyMatch(t -> STRING_TYPES.contains(t.getType().getSqlTypeName()));

        if (!containValidatedType) {
            return row;
        }

        int colCount = rowType.getFieldList().size();
        RowBuilder<RowT> rowBldr = null;

        for (int i = 0; i < colCount; ++i) {
            RelDataType colType = rowType.getFieldList().get(i).getType();
            SqlTypeName typeName = colType.getSqlTypeName();
            Object data = rowHandler.get(i, row);

            if (data == null || (!BINARY_TYPES.contains(typeName) && !CHAR_TYPES.contains(typeName))) {
                if (rowBldr != null) {
                    rowBldr.addField(data);
                }
                continue;
            }

            int colPrecision = colType.getPrecision();

            assert colPrecision != RelDataType.PRECISION_NOT_SPECIFIED;

            // Validate and trim if needed.

            if (BINARY_TYPES.contains(typeName)) {
                assert data instanceof ByteString;

                ByteString byteString = (ByteString) data;

                if (byteString.length() > colPrecision) {
                    for (int pos = byteString.length(); pos > colPrecision; --pos) {
                        if (byteString.byteAt(pos - 1) != 0) {
                            throw new SqlException(STMT_VALIDATION_ERR, "Value too long for type: " + colType);
                        }
                    }

                    data = byteString.substring(0, colPrecision);

                    if (rowBldr == null) {
                        rowBldr = buildPartialRow(rowHandler, schema, i, row);
                    }
                }
            }

            if (CHAR_TYPES.contains(typeName)) {
                assert data instanceof String;

                String str = (String) data;

                if (str.length() > colPrecision) {
                    for (int pos = str.length(); pos > colPrecision; --pos) {
                        if (str.charAt(pos - 1) != ' ') {
                            throw new SqlException(STMT_VALIDATION_ERR, "Value too long for type: " + colType);
                        }
                    }

                    data = str.substring(0, colPrecision);

                    if (rowBldr == null) {
                        rowBldr = buildPartialRow(rowHandler, schema, i, row);
                    }
                }
            }

            if (rowBldr != null) {
                rowBldr.addField(data);
            }
        }

        if (rowBldr != null) {
            return rowBldr.build();
        } else {
            return row;
        }
    }

    private static <RowT> RowBuilder<RowT> buildPartialRow(RowHandler<RowT> rowHandler, Supplier<RowSchema> schema, int endPos, RowT row) {
        RowFactory<RowT> factory = rowHandler.factory(schema.get());
        RowBuilder<RowT> bldr = factory.rowBuilder();

        for (int i = 0; i < endPos; ++i) {
            Object data = rowHandler.get(i, row);

            bldr.addField(data);
        }

        return bldr;
    }

    /**
     * Checks whether or not the given types represent the same column types.
     *
     * @param lhs Left type.
     * @param rhs Right type.
     * @return {@code true} if types represent the same {@link ColumnType} after conversion.
     */
    // TODO this method can be removed after https://issues.apache.org/jira/browse/IGNITE-22295
    public static boolean typesRepresentTheSameColumnTypes(RelDataType lhs, RelDataType rhs) {
        ColumnType col1 = columnType(lhs);
        ColumnType col2 = columnType(rhs);

        return col1 == col2;
    }

    /**
     * Returns {@code true} if the specified type name is
     * {@link SqlTypeName#TIMESTAMP} or {@link SqlTypeName#TIMESTAMP_WITH_LOCAL_TIME_ZONE}.
     */
    public static boolean isTimestamp(SqlTypeName typeName) {
        return typeName == SqlTypeName.TIMESTAMP || typeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    }
}
