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
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.runtime.Geometries;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.ignite.schema.definition.ColumnType;

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
                    return Geometries.Geom.class;
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

    /**
     * Gets ColumnType type for RelDataType.
     *
     * @param relType Rel type.
     * @return ColumnType type or null.
     */
    public static ColumnType relDataTypeToColumnType(RelDataType relType) {
        assert relType instanceof BasicSqlType
                || relType instanceof IntervalSqlType : "Not supported.";

        switch (relType.getSqlTypeName()) {
            case BOOLEAN:
                //TODO: https://issues.apache.org/jira/browse/IGNITE-17298
                throw new IllegalArgumentException("Type is not supported yet.");
            case TINYINT:
                return ColumnType.INT8;
            case SMALLINT:
                return ColumnType.INT16;
            case INTEGER:
                return ColumnType.INT32;
            case BIGINT:
                return ColumnType.INT64;
            case DECIMAL:
                assert relType.getPrecision() != PRECISION_NOT_SPECIFIED;

                return ColumnType.decimalOf(relType.getPrecision(), relType.getScale());
            case FLOAT:
            case REAL:
                return ColumnType.FLOAT;
            case DOUBLE:
                return ColumnType.DOUBLE;
            case DATE:
                return ColumnType.DATE;
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                return relType.getPrecision() == PRECISION_NOT_SPECIFIED ? ColumnType.time() :
                        ColumnType.time(relType.getPrecision());
            case TIMESTAMP:
                return relType.getPrecision() == PRECISION_NOT_SPECIFIED ? ColumnType.datetime() :
                        ColumnType.datetime(relType.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return relType.getPrecision() == PRECISION_NOT_SPECIFIED ? ColumnType.timestamp() :
                        ColumnType.timestamp(relType.getPrecision());
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
                //TODO: https://issues.apache.org/jira/browse/IGNITE-17373
                throw new IllegalArgumentException("Type is not supported yet.");
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
                throw new IllegalArgumentException("Type is not supported yet.");
            case VARCHAR:
            case CHAR:
                return relType.getPrecision() == PRECISION_NOT_SPECIFIED ? ColumnType.string() :
                        ColumnType.stringOf(relType.getPrecision());
            case BINARY:
            case VARBINARY:
                return relType.getPrecision() == PRECISION_NOT_SPECIFIED ? ColumnType.blob() :
                        ColumnType.blobOf(relType.getPrecision());
            default:
                throw new IllegalArgumentException("Type is not supported.");
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
                    return Geometries.Geom.class;
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

    /** {@inheritDoc} */
    @Override
    public RelDataType leastRestrictive(List<RelDataType> types) {
        assert types != null;
        assert types.size() >= 1;

        if (types.size() == 1 || allEquals(types)) {
            return first(types);
        }

        return super.leastRestrictive(types);
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

            if (clazz == Duration.class) {
                return createTypeWithNullability(createSqlIntervalType(INTERVAL_QUALIFIER_DAY_TIME), true);
            } else if (clazz == Period.class) {
                return createTypeWithNullability(createSqlIntervalType(INTERVAL_QUALIFIER_YEAR_MONTH), true);
            }
        }

        return super.toSql(type);
    }

    /** {@inheritDoc} */
    @Override public RelDataType createType(Type type) {
        if (type == Duration.class || type == Period.class || type == LocalDate.class || type == LocalDateTime.class
                || type == LocalTime.class) {
            return createJavaType((Class<?>) type);
        }

        return super.createType(type);
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
}
