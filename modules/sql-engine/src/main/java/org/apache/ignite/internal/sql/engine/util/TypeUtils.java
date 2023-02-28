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

import static org.apache.ignite.internal.sql.engine.util.Commons.transform;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.ignite.internal.schema.DecimalNativeType;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NumberNativeType;
import org.apache.ignite.internal.schema.TemporalNativeType;
import org.apache.ignite.internal.schema.VarlenNativeType;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.type.IgniteCustomType;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * TypeUtils.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class TypeUtils {
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

    /**
     * NeedCast.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static boolean needCast(RelDataTypeFactory factory, RelDataType fromType, RelDataType toType) {
        // This prevents that we cast a JavaType to normal RelDataType.
        if (fromType instanceof RelDataTypeFactoryImpl.JavaType
                && toType.getSqlTypeName() == fromType.getSqlTypeName()) {
            return false;
        }

        // Do not make a cast when we don't know specific type (ANY) of the origin node.
        if (toType.getSqlTypeName() == SqlTypeName.ANY
                || fromType.getSqlTypeName() == SqlTypeName.ANY) {
            return false;
        }

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
        if (SqlTypeUtil.equalSansNullability(factory, fromType, toType)) {
            return false;
        }

        // Should keep sync with rules in SqlTypeCoercionRule.
        assert SqlTypeUtil.canCastFrom(toType, fromType, true);

        return true;
    }

    /**
     * CreateRowType.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @NotNull
    public static RelDataType createRowType(@NotNull IgniteTypeFactory typeFactory, @NotNull Class<?>... fields) {
        List<RelDataType> types = Arrays.stream(fields)
                .map(typeFactory::createJavaType)
                .collect(Collectors.toList());

        return createRowType(typeFactory, types, "$F");
    }

    /**
     * CreateRowType.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @NotNull
    public static RelDataType createRowType(@NotNull IgniteTypeFactory typeFactory, @NotNull RelDataType... fields) {
        List<RelDataType> types = Arrays.asList(fields);

        return createRowType(typeFactory, types, "$F");
    }

    private static RelDataType createRowType(IgniteTypeFactory typeFactory, List<RelDataType> fields, String namePreffix) {
        List<String> names = IntStream.range(0, fields.size())
                .mapToObj(ord -> namePreffix + ord)
                .collect(Collectors.toList());

        return typeFactory.createStructType(fields, names);
    }

    /**
     * Function.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static <RowT> Function<RowT, RowT> resultTypeConverter(ExecutionContext<RowT> ectx, RelDataType resultType) {
        assert resultType.isStruct();

        if (hasConvertableFields(resultType)) {
            RowHandler<RowT> handler = ectx.rowHandler();
            List<RelDataType> types = RelOptUtil.getFieldTypeList(resultType);
            RowHandler.RowFactory<RowT> factory = handler.factory(ectx.getTypeFactory(), types);
            List<Function<Object, Object>> converters = transform(types, t -> fieldConverter(ectx, t));
            return r -> {
                RowT newRow = factory.create();
                assert handler.columnCount(newRow) == converters.size();
                assert handler.columnCount(r) == converters.size();
                for (int i = 0; i < converters.size(); i++) {
                    handler.set(i, newRow, converters.get(i).apply(handler.get(i, r)));
                }
                return newRow;
            };
        }

        return Function.identity();
    }

    private static Function<Object, Object> fieldConverter(ExecutionContext<?> ectx, RelDataType fieldType) {
        Type storageType = ectx.getTypeFactory().getResultClass(fieldType);

        if (isConvertableType(fieldType)) {
            return v -> fromInternal(ectx, v, storageType);
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
        return RelOptUtil.getFieldTypeList(resultType).stream()
                .anyMatch(TypeUtils::isConvertableType);
    }

    /**
     * ToInternal. Converts the given value to its presentation used by the execution engine.
     *
     * @deprecated The implementation of this method is incorrect because it relies on the assumption that
     *      {@code val.getClass() == storageType(val)} is always true, which sometimes is not the case.
     *      Use {@link #toInternal(ExecutionContext, Object, Type)} that provides type information instead.
     */
    @Deprecated
    public static @Nullable Object toInternal(ExecutionContext<?> ectx, Object val) {
        return val == null ? null : toInternal(ectx, val, val.getClass());
    }

    /**
     * ToInternal.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static @Nullable Object toInternal(ExecutionContext<?> ectx, Object val, Type storageType) {
        if (val == null) {
            return null;
        } else if (storageType == LocalDate.class) {
            return (int) ((LocalDate) val).toEpochDay();
        } else if (storageType == LocalTime.class) {
            return (int) (TimeUnit.NANOSECONDS.toMillis(((LocalTime) val).toNanoOfDay()));
        } else if (storageType == LocalDateTime.class) {
            var dt = (LocalDateTime) val;

            return TimeUnit.SECONDS.toMillis(dt.toEpochSecond(ZoneOffset.UTC)) + TimeUnit.NANOSECONDS.toMillis(dt.getNano());
        } else if (storageType == Duration.class) {
            return TimeUnit.SECONDS.toMillis(((Duration) val).getSeconds())
                    + TimeUnit.NANOSECONDS.toMillis(((Duration) val).getNano());
        } else if (storageType == Period.class) {
            return (int) ((Period) val).toTotalMonths();
        } else if (storageType == byte[].class) {
            return new ByteString((byte[]) val);
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
            // TODO: https://issues.apache.org/jira/browse/IGNITE-17298 SQL: Support BOOLEAN datatype.
            //   Fix this after BOOLEAN type supported is implemented.
            if (storageType == Boolean.class) {
                return val;
            }
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
    public static @Nullable Object fromInternal(ExecutionContext<?> ectx, Object val, Type storageType) {
        if (val == null) {
            return null;
        } else if (storageType == LocalDate.class && val instanceof Integer) {
            return LocalDate.ofEpochDay((Integer) val);
        } else if (storageType == LocalTime.class && val instanceof Integer) {
            return LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(Long.valueOf((Integer) val)));
        } else if (storageType == LocalDateTime.class && (val instanceof Long)) {
            return LocalDateTime.ofEpochSecond(TimeUnit.MILLISECONDS.toSeconds((Long) val),
                    (int) TimeUnit.MILLISECONDS.toNanos((Long) val % 1000), ZoneOffset.UTC);
        } else if (storageType == Duration.class && val instanceof Long) {
            return Duration.ofMillis((Long) val);
        } else if (storageType == Period.class && val instanceof Integer) {
            return Period.of((Integer) val / 12, (Integer) val % 12, 0);
        } else if (storageType == byte[].class && val instanceof ByteString) {
            return ((ByteString) val).getBytes();
        } else {
            // TODO: https://issues.apache.org/jira/browse/IGNITE-17298 SQL: Support BOOLEAN datatype.
            //   Fix this after BOOLEAN type supported is implemented.
            if (storageType == Boolean.class) {
                return val;
            }
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
     * Converts a {@link NativeType native type} to {@link RelDataType relational type}.
     *
     * @param factory Type factory.
     * @param nativeType A native type to convert.
     * @return Relational type.
     */
    public static RelDataType native2relationalType(RelDataTypeFactory factory, NativeType nativeType) {
        switch (nativeType.spec()) {
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
                // TODO IGNITE-18431.
                throw new AssertionError("BITMASK is not supported yet");
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
}
