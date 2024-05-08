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

import static org.apache.calcite.sql.type.SqlTypeName.CHAR_TYPES;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import java.lang.reflect.Type;
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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.row.BaseTypeSpec;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchemaTypes;
import org.apache.ignite.internal.sql.engine.exec.row.RowType;
import org.apache.ignite.internal.sql.engine.exec.row.TypeSpec;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomType;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomTypeCoercionRules;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.apache.ignite.internal.type.BitmaskNativeType;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.NumberNativeType;
import org.apache.ignite.internal.type.TemporalNativeType;
import org.apache.ignite.internal.type.VarlenNativeType;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * TypeUtils.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class TypeUtils {
    public static final BiFunction<Integer, Object, Object> BI_FUNCTION_IDENTITY_SECOND_ARGUMENT = (idx, r) -> r;

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

    private static class SupportedParamClassesHolder {
        static final Set<Class<?>> supportedParamClasses;

        static {
            supportedParamClasses = Arrays.stream(ColumnType.values()).map(ColumnType::javaClass).collect(Collectors.toSet());
            supportedParamClasses.add(boolean.class);
            supportedParamClasses.add(byte.class);
            supportedParamClasses.add(short.class);
            supportedParamClasses.add(int.class);
            supportedParamClasses.add(long.class);
            supportedParamClasses.add(float.class);
            supportedParamClasses.add(double.class);
        }
    }

    private static Set<Class<?>> supportedParamClasses() {
        return SupportedParamClassesHolder.supportedParamClasses;
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
     * @param ectx SQL execution context.
     * @param resultType Type of result.
     * @return Function for two arguments. First argument is an index of column to convert. Second argument is a value to be converted
     */
    public static BiFunction<Integer, Object, Object> resultTypeConverter(ExecutionContext<?> ectx, RelDataType resultType) {
        assert resultType.isStruct();

        if (hasConvertableFields(resultType)) {
            List<RelDataType> types = RelOptUtil.getFieldTypeList(resultType);
            Function<Object, Object>[] converters = (Function<Object, Object>[]) new Function[types.size()];
            for (int i = 0; i < types.size(); i++) {
                converters[i] = fieldConverter(ectx, types.get(i));
            }

            return (idx, r) -> {
                assert idx >= 0 && idx < converters.length;
                return converters[idx].apply(r);
            };
        }

        return BI_FUNCTION_IDENTITY_SECOND_ARGUMENT;
    }

    private static Function<Object, Object> fieldConverter(ExecutionContext<?> ectx, RelDataType fieldType) {
        Type storageType = ectx.getTypeFactory().getResultClass(fieldType);

        if (isConvertableType(fieldType)) {
            return v -> fromInternal(v, storageType);
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
    public static @Nullable Object toInternal(@Nullable Object val, Type storageType) {
        if (val == null) {
            return null;
        } else if (storageType == LocalDate.class) {
            return (int) ((LocalDate) val).toEpochDay();
        } else if (storageType == LocalTime.class) {
            return (int) (TimeUnit.NANOSECONDS.toMillis(((LocalTime) val).toNanoOfDay()));
        } else if (storageType == LocalDateTime.class) {
            var dt = (LocalDateTime) val;

            return TimeUnit.SECONDS.toMillis(dt.toEpochSecond(ZoneOffset.UTC)) + TimeUnit.NANOSECONDS.toMillis(dt.getNano());
        } else if (storageType == Instant.class) {
            var timeStamp = (Instant) val;

            return timeStamp.toEpochMilli();
        } else if (storageType == Duration.class) {
            return TimeUnit.SECONDS.toMillis(((Duration) val).getSeconds())
                    + TimeUnit.NANOSECONDS.toMillis(((Duration) val).getNano());
        } else if (storageType == Period.class) {
            return (int) ((Period) val).toTotalMonths();
        } else if (storageType == byte[].class) {
            if (val instanceof String) {
                return new ByteString(((String) val).getBytes(StandardCharsets.UTF_8));
            } else if (val instanceof byte[]) {
                return new ByteString((byte[]) val);
            } else {
                assert val instanceof ByteString : "Expected ByteString but got " + val + ", type=" + val.getClass().getTypeName();
                return val;
            }
        } else if (val instanceof Number && storageType != val.getClass()) {
            // For dynamic parameters we don't know exact parameter type in compile time. To avoid casting errors in
            // runtime we should convert parameter value to expected type.
            Number num = (Number) val;

            return Byte.class.equals(storageType) || byte.class.equals(storageType) ? SqlFunctions.toByte(num) :
                    Short.class.equals(storageType) || short.class.equals(storageType) ? SqlFunctions.toShort(num) :
                            Integer.class.equals(storageType) || int.class.equals(storageType) ? SqlFunctions.toInt(num) :
                                    Long.class.equals(storageType) || long.class.equals(storageType) ? SqlFunctions.toLong(num) :
                                            Float.class.equals(storageType) || float.class.equals(storageType) ? SqlFunctions.toFloat(num) :
                                                    Double.class.equals(storageType) || double.class.equals(storageType)
                                                            ? SqlFunctions.toDouble(num) :
                                                            BigDecimal.class.equals(storageType) ? SqlFunctions.toBigDecimal(num) : num;
        } else {
            var nativeTypeSpec = NativeTypeSpec.fromClass((Class<?>) storageType);
            assert nativeTypeSpec != null : "No native type spec for type: " + storageType;

            var customType = SafeCustomTypeInternalConversion.INSTANCE.tryConvertToInternal(val, nativeTypeSpec);
            return customType != null ? customType : val;
        }
    }

    /**
     * FromInternal.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static @Nullable Object fromInternal(@Nullable Object val, Type storageType) {
        if (val == null) {
            return null;
        } else if (storageType == LocalDate.class && val instanceof Integer) {
            return LocalDate.ofEpochDay((Integer) val);
        } else if (storageType == LocalTime.class && val instanceof Integer) {
            return LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(Long.valueOf((Integer) val)));
        } else if (storageType == LocalDateTime.class && (val instanceof Long)) {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli((long) val), ZoneOffset.UTC);
        } else if (storageType == Instant.class && val instanceof Long) {
            return Instant.ofEpochMilli((long) val);
        } else if (storageType == Duration.class && val instanceof Long) {
            return Duration.ofMillis((Long) val);
        } else if (storageType == Period.class && val instanceof Integer) {
            return Period.of((Integer) val / 12, (Integer) val % 12, 0);
        } else if (storageType == byte[].class && val instanceof ByteString) {
            return ((ByteString) val).getBytes();
        } else {
            var nativeTypeSpec = NativeTypeSpec.fromClass((Class<?>) storageType);
            assert nativeTypeSpec != null : "No native type spec for type: " + storageType;

            var customType = SafeCustomTypeInternalConversion.INSTANCE.tryConvertFromInternal(val, nativeTypeSpec);
            return customType != null ? customType : val;
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
                if (type instanceof IgniteCustomType) {
                    IgniteCustomType customType = (IgniteCustomType) type;
                    return customType.spec().columnType();
                }
                // fallthrough
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
            default:
                assert false : "Unexpected type of result: " + type.getSqlTypeName();
                return null;
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
                IgniteTypeFactory concreteTypeFactory = (IgniteTypeFactory) factory;
                return concreteTypeFactory.createCustomType(UuidType.NAME);
            case STRING: {
                assert nativeType instanceof VarlenNativeType;

                var varlen = (VarlenNativeType) nativeType;

                return factory.createSqlType(SqlTypeName.VARCHAR, varlen.length());
            }
            case BYTES: {
                assert nativeType instanceof VarlenNativeType;

                var varlen = (VarlenNativeType) nativeType;

                return factory.createSqlType(SqlTypeName.VARBINARY, varlen.length());
            }
            case BITMASK:
                assert nativeType instanceof BitmaskNativeType;

                var bitmask = (BitmaskNativeType) nativeType;

                // TODO IGNITE-18431.
                return factory.createSqlType(SqlTypeName.VARBINARY, bitmask.sizeInBytes());
            case NUMBER:
                assert nativeType instanceof NumberNativeType;

                var number = (NumberNativeType) nativeType;

                return factory.createSqlType(SqlTypeName.DECIMAL, number.precision(), 0);
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
            case BITMASK:
                return NativeTypes.bitmaskOf(length);
            case STRING:
                return NativeTypes.stringOf(length);
            case BYTE_ARRAY:
                return NativeTypes.blobOf(length);
            case NUMBER:
                return NativeTypes.numberOf(precision);
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

        // Implicit type coercion does not handle nullability.
        if (SqlTypeUtil.equalSansNullability(typeFactory, fromType, toType)) {
            return false;
        }
        // Should keep sync with rules in SqlTypeCoercionRule.
        assert SqlTypeUtil.canCastFrom(toType, fromType, true);
        return true;
    }

    /**
     * Checks whether one type can be casted to another if one of type is a custom data type.
     *
     * <p>This method expects at least one of its arguments to be a custom data type.
     */
    public static boolean customDataTypeNeedCast(IgniteTypeFactory factory, RelDataType fromType, RelDataType toType) {
        IgniteCustomTypeCoercionRules typeCoercionRules = factory.getCustomTypeCoercionRules();
        if (toType instanceof IgniteCustomType) {
            IgniteCustomType to = (IgniteCustomType) toType;
            return typeCoercionRules.needToCast(fromType, to);
        } else if (fromType instanceof IgniteCustomType) {
            boolean sameType = SqlTypeUtil.equalSansNullability(fromType, toType);
            return !sameType;
        } else {
            String message = format("Invalid arguments. Expected at least one custom data type but got {} and {}", fromType, toType);
            throw new AssertionError(message);
        }
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
        } else if (fromType instanceof IgniteCustomType && toType instanceof IgniteCustomType) {
            IgniteCustomType fromCustom = (IgniteCustomType) fromType;
            IgniteCustomType toCustom = (IgniteCustomType) toType;

            // IgniteCustomType: different custom data types are not compatible.
            return Objects.equals(fromCustom.getCustomTypeName(), toCustom.getCustomTypeName());
        } else if (fromType instanceof IgniteCustomType || toType instanceof IgniteCustomType) {
            // IgniteCustomType: custom data types are not compatible with other types.
            return false;
        } else if (SqlTypeUtil.canAssignFrom(toType, fromType)) {
            return SqlTypeUtil.canAssignFrom(fromType, toType);
        } else {
            return false;
        }
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

        if (type instanceof IgniteCustomType) {
            NativeType nativeType = IgniteTypeFactory.relDataTypeToNative(type);
            return RowSchemaTypes.nativeTypeWithNullability(nativeType, nullable);
        } else if (SqlTypeName.ANY == type.getSqlTypeName()) {
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
                return RowSchemaTypes.nativeTypeWithNullability(NativeTypes.INT32, nullable);
            } else {
                // DAY interval is stored as time as long.
                return RowSchemaTypes.nativeTypeWithNullability(NativeTypes.INT64, nullable);
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

    /** Check limitation for character types and throws exception if row contains character sequence more than type defined. */
    public static <RowT> void validateCharactersOverflow(RelDataType rowType, RowHandler<RowT> rowHandler, RowT row) {
        int colCount = rowType.getFieldList().size();

        for (int i = 0; i < colCount; ++i) {
            RelDataType colType = rowType.getFieldList().get(i).getType();
            if (CHAR_TYPES.contains(colType.getSqlTypeName())) {
                Object data = rowHandler.get(i, row);

                if (data == null) {
                    continue;
                }

                assert data instanceof String;

                String str = (String) data;

                int colPrecision = colType.getPrecision();

                assert colPrecision != RelDataType.PRECISION_NOT_SPECIFIED;

                if (str.stripTrailing().length() > colPrecision) {
                    throw new SqlException(STMT_VALIDATION_ERR, "Value too long for type: " + colType);
                }
            }
        }
    }
}
