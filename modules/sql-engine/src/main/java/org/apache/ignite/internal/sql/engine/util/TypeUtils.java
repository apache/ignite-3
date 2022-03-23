/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.apache.calcite.util.Util.last;
import static org.apache.ignite.internal.sql.engine.util.Commons.nativeTypeToClass;
import static org.apache.ignite.internal.sql.engine.util.Commons.transform;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.DataContext;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * TypeUtils.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class TypeUtils {
    private static final Set<Type> CONVERTABLE_TYPES = Set.of(
            LocalDate.class,
            LocalDateTime.class,
            LocalTime.class,
            Timestamp.class,
            Duration.class,
            Period.class
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
     * SqlType.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static RelDataType sqlType(IgniteTypeFactory typeFactory, RelDataType rowType) {
        if (!rowType.isStruct()) {
            return typeFactory.toSql(rowType);
        }

        return typeFactory.createStructType(
                transform(rowType.getFieldList(),
                        f -> Pair.of(f.getName(), sqlType(typeFactory, f.getType()))));
    }

    /**
     * GetResultType.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param schema  Schema.
     * @param sqlType Logical row type.
     * @param origins Columns origins.
     * @return Result type.
     */
    public static RelDataType getResultType(IgniteTypeFactory typeFactory, RelOptSchema schema, RelDataType sqlType,
            @Nullable List<List<String>> origins) {
        assert origins == null || origins.size() == sqlType.getFieldCount();

        RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(typeFactory);
        List<RelDataTypeField> fields = sqlType.getFieldList();

        for (int i = 0; i < sqlType.getFieldCount(); i++) {
            List<String> origin = origins == null ? null : origins.get(i);
            b.add(fields.get(i).getName(), typeFactory.createType(
                    getResultClass(typeFactory, schema, fields.get(i).getType(), origin)));
        }

        return b.build();
    }

    /**
     * GetResultClass.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param schema Schema.
     * @param type   Logical column type.
     * @param origin Column origin.
     * @return Result type.
     */
    private static Type getResultClass(IgniteTypeFactory typeFactory, RelOptSchema schema, RelDataType type,
            @Nullable List<String> origin) {
        if (nullOrEmpty(origin)) {
            return typeFactory.getResultClass(type);
        }

        RelOptTable table = schema.getTableForMember(origin.subList(0, origin.size() - 1));

        assert table != null;

        ColumnDescriptor fldDesc = table.unwrap(TableDescriptor.class).columnDescriptor(last(origin));

        assert fldDesc != null;

        return nativeTypeToClass(fldDesc.physicalType());
    }

    /**
     * Function.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static <RowT> Function<RowT, RowT> resultTypeConverter(ExecutionContext<RowT> ectx, RelDataType resultType) {
        assert resultType.isStruct();

        resultType = TypeUtils.getResultType(Commons.typeFactory(), ectx.unwrap(BaseQueryContext.class).catalogReader(), resultType, null);

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
        Type storageType = ectx.getTypeFactory().getJavaClass(fieldType);

        if (isConvertableType(storageType)) {
            return v -> fromInternal(ectx, v, storageType);
        }

        return Function.identity();
    }

    /**
     * IsConvertableType.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static boolean isConvertableType(Type type) {
        return CONVERTABLE_TYPES.contains(type);
    }

    /**
     * IsConvertableType.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static boolean isConvertableType(RelDataType type) {
        return type instanceof RelDataTypeFactoryImpl.JavaType
                && isConvertableType(((RelDataTypeFactoryImpl.JavaType) type).getJavaClass());
    }

    private static boolean hasConvertableFields(RelDataType resultType) {
        return RelOptUtil.getFieldTypeList(resultType).stream()
                .anyMatch(TypeUtils::isConvertableType);
    }

    /**
     * ToInternal.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static Object toInternal(ExecutionContext<?> ectx, Object val) {
        return val == null ? null : toInternal(ectx, val, val.getClass());
    }

    /**
     * ToInternal.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static Object toInternal(ExecutionContext<?> ectx, Object val, Type storageType) {
        if (val == null) {
            return null;
        } else if (storageType == java.sql.Date.class) {
            return (int) (SqlFunctions.toLong((java.util.Date) val, DataContext.Variable.TIME_ZONE.get(ectx))
                    / DateTimeUtils.MILLIS_PER_DAY);
        } else if (storageType == java.sql.Time.class) {
            return (int) (SqlFunctions.toLong((java.util.Date) val, DataContext.Variable.TIME_ZONE.get(ectx))
                    % DateTimeUtils.MILLIS_PER_DAY);
        } else if (storageType == Timestamp.class) {
            return SqlFunctions.toLong((java.util.Date) val, DataContext.Variable.TIME_ZONE.get(ectx));
        } else if (storageType == java.util.Date.class) {
            return SqlFunctions.toLong((java.util.Date) val, DataContext.Variable.TIME_ZONE.get(ectx));
        } else if (storageType == Duration.class) {
            return TimeUnit.SECONDS.toMillis(((Duration) val).getSeconds())
                    + TimeUnit.NANOSECONDS.toMillis(((Duration) val).getNano());
        } else if (storageType == Period.class) {
            return (int) ((Period) val).toTotalMonths();
        } else {
            return val;
        }
    }

    /**
     * FromInternal.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static Object fromInternal(ExecutionContext<?> ectx, Object val, Type storageType) {
        if (val == null) {
            return null;
        } else if (storageType == LocalDate.class && val instanceof Integer) {
            return LocalDate.ofEpochDay((Integer) val);
        } else if (storageType == LocalTime.class && val instanceof Integer) {
            return LocalTime.ofSecondOfDay((Integer) val / 1000);
        } else if (storageType == LocalDateTime.class && (val instanceof Long)) {
            return LocalDateTime.ofEpochSecond((Long) val / 1000, (int) ((Long) val % 1000) * 1000 * 1000, ZoneOffset.UTC);
        } else if (storageType == Duration.class && val instanceof Long) {
            return Duration.ofMillis((Long) val);
        } else if (storageType == Period.class && val instanceof Integer) {
            return Period.of((Integer) val / 12, (Integer) val % 12, 0);
        } else if (storageType == byte[].class && val instanceof ByteString) {
            return ((ByteString) val).getBytes();
        } else {
            return val;
        }
    }

    /**
     * Convert calcite date type to Ignite native type.
     */
    public static NativeType nativeType(RelDataType type) {
        switch (type.getSqlTypeName()) {
            case VARCHAR:
            case CHAR:
                return type.getPrecision() > 0 ? NativeTypes.stringOf(type.getPrecision()) : NativeTypes.STRING;
            case DATE:
                return NativeTypes.DATE;
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                return type.getPrecision() > 0 ? NativeTypes.time(type.getPrecision()) : NativeTypes.time();
            case INTEGER:
                return NativeTypes.INT32;
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return type.getPrecision() > 0 ? NativeTypes.timestamp(type.getPrecision()) : NativeTypes.timestamp();
            case BIGINT:
                return NativeTypes.INT64;
            case SMALLINT:
                return NativeTypes.INT16;
            case TINYINT:
                return NativeTypes.INT8;
            case DECIMAL:
                return NativeTypes.decimalOf(type.getPrecision(), type.getScale());
            case BOOLEAN:
                return NativeTypes.INT8;
            case DOUBLE:
                return NativeTypes.DOUBLE;
            case REAL:
            case FLOAT:
                return NativeTypes.FLOAT;
            case BINARY:
            case VARBINARY:
                return NativeTypes.blobOf(type.getPrecision());
            case ANY:
            case OTHER:
                return NativeTypes.blobOf(type.getPrecision());
            default:
                assert false : "Unexpected type of result: " + type.getSqlTypeName();
                return null;
        }
    }
}
