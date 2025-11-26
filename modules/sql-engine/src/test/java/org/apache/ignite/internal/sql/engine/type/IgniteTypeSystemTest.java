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

import static org.apache.ignite.internal.sql.engine.type.IgniteTypeSystem.MIN_SCALE_OF_AVG_RESULT;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.native2relationalType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.sql.ColumnType;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for {@link IgniteTypeSystem}. */
public class IgniteTypeSystemTest extends BaseIgniteAbstractTest {

    private static final int TIME_PRECISION = 9;
    private static final int STRING_PRECISION = Integer.MAX_VALUE;
    private static final int DECIMAL_PRECISION = 32767;
    private static final int DECIMAL_SCALE = 32767;
    private static final int TIMESTAMP_DEFAULT_PRECISION = 6;
    private final IgniteTypeSystem typeSystem = IgniteTypeSystem.INSTANCE;

    @Test
    public void getMaxPrecision() {
        assertEquals(STRING_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.CHAR));
        assertEquals(STRING_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.VARCHAR));
        assertEquals(STRING_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.BINARY));
        assertEquals(STRING_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.VARBINARY));

        assertEquals(3, typeSystem.getMaxPrecision(SqlTypeName.TINYINT));
        assertEquals(5, typeSystem.getMaxPrecision(SqlTypeName.SMALLINT));
        assertEquals(10, typeSystem.getMaxPrecision(SqlTypeName.INTEGER));
        assertEquals(19, typeSystem.getMaxPrecision(SqlTypeName.BIGINT));
        assertEquals(7, typeSystem.getMaxPrecision(SqlTypeName.REAL));
        assertEquals(14, typeSystem.getMaxPrecision(SqlTypeName.FLOAT));
        assertEquals(15, typeSystem.getMaxPrecision(SqlTypeName.DOUBLE));
        assertEquals(DECIMAL_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.DECIMAL));

        assertEquals(0, typeSystem.getMaxPrecision(SqlTypeName.DATE));
        assertEquals(TIME_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.TIME));
        assertEquals(TIME_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE));
        assertEquals(TIME_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.TIMESTAMP));
        assertEquals(TIME_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE));
    }

    @Test
    public void getDefaultPrecision() {
        assertEquals(1, typeSystem.getDefaultPrecision(SqlTypeName.CHAR));
        assertEquals(RelDataType.PRECISION_NOT_SPECIFIED, typeSystem.getDefaultPrecision(SqlTypeName.VARCHAR));
        assertEquals(1, typeSystem.getDefaultPrecision(SqlTypeName.BINARY));
        assertEquals(RelDataType.PRECISION_NOT_SPECIFIED, typeSystem.getDefaultPrecision(SqlTypeName.VARBINARY));

        assertEquals(3, typeSystem.getDefaultPrecision(SqlTypeName.TINYINT));
        assertEquals(5, typeSystem.getDefaultPrecision(SqlTypeName.SMALLINT));
        assertEquals(10, typeSystem.getDefaultPrecision(SqlTypeName.INTEGER));
        assertEquals(19, typeSystem.getDefaultPrecision(SqlTypeName.BIGINT));
        assertEquals(7, typeSystem.getDefaultPrecision(SqlTypeName.REAL));
        assertEquals(14, typeSystem.getDefaultPrecision(SqlTypeName.FLOAT));
        assertEquals(15, typeSystem.getDefaultPrecision(SqlTypeName.DOUBLE));
        assertEquals(DECIMAL_PRECISION, typeSystem.getDefaultPrecision(SqlTypeName.DECIMAL));

        assertEquals(0, typeSystem.getDefaultPrecision(SqlTypeName.DATE));
        assertEquals(0, typeSystem.getDefaultPrecision(SqlTypeName.TIME));
        assertEquals(0, typeSystem.getDefaultPrecision(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE));
        assertEquals(TIMESTAMP_DEFAULT_PRECISION, typeSystem.getDefaultPrecision(SqlTypeName.TIMESTAMP));
        assertEquals(TIMESTAMP_DEFAULT_PRECISION, typeSystem.getDefaultPrecision(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE));
    }

    @Test
    public void testGetMaxNumericPrecision() {
        assertEquals(DECIMAL_PRECISION, typeSystem.getMaxNumericPrecision());
    }

    @Test
    public void testGetMaxNumericScale() {
        assertEquals(DECIMAL_SCALE, typeSystem.getMaxNumericScale());
    }

    @ParameterizedTest
    @MethodSource("deriveAvgTypeArguments")
    void deriveAvgType(RelDataType argument, RelDataType expected) {
        RelDataType actual = typeSystem.deriveAvgAggType(Commons.typeFactory(), argument);

        assertThat(actual, Matchers.equalTo(expected));
    }

    private static Stream<Arguments> deriveAvgTypeArguments() {
        IgniteTypeSystem typeSystem = IgniteTypeSystem.INSTANCE;
        IgniteTypeFactory typeFactory = Commons.typeFactory();

        return Stream.of(
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.INT8),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(
                                typeSystem.getMaxPrecision(SqlTypeName.TINYINT) + MIN_SCALE_OF_AVG_RESULT, MIN_SCALE_OF_AVG_RESULT
                        ))
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.INT16),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(
                                typeSystem.getMaxPrecision(SqlTypeName.SMALLINT) + MIN_SCALE_OF_AVG_RESULT, MIN_SCALE_OF_AVG_RESULT
                        ))
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.INT32),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(
                                typeSystem.getMaxPrecision(SqlTypeName.INTEGER) + MIN_SCALE_OF_AVG_RESULT, MIN_SCALE_OF_AVG_RESULT
                        ))
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.INT64),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(
                                typeSystem.getMaxPrecision(SqlTypeName.BIGINT) + MIN_SCALE_OF_AVG_RESULT, MIN_SCALE_OF_AVG_RESULT
                        ))
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.decimalOf(4, 0)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(
                                4 + MIN_SCALE_OF_AVG_RESULT, MIN_SCALE_OF_AVG_RESULT
                        ))
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.decimalOf(4, 2)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(
                                2 + MIN_SCALE_OF_AVG_RESULT, MIN_SCALE_OF_AVG_RESULT
                        ))
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.decimalOf(4, 4)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(
                                MIN_SCALE_OF_AVG_RESULT, MIN_SCALE_OF_AVG_RESULT
                        ))
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.decimalOf(20, 18)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(20, 18))
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.FLOAT),
                        native2relationalType(typeFactory, NativeTypes.DOUBLE)
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.DOUBLE),
                        native2relationalType(typeFactory, NativeTypes.DOUBLE)
                )
        );
    }

    @ParameterizedTest
    @MethodSource("deriveDivideDecimalArgs")
    void deriveDivide(RelDataType a1, RelDataType a2, RelDataType rt) {
        RelDataType actual = typeSystem.deriveDecimalDivideType(Commons.typeFactory(), a1, a2);

        assertThat(actual, Matchers.equalTo(rt));
    }

    private static Stream<Arguments> deriveDivideDecimalArgs() {
        IgniteTypeFactory typeFactory = Commons.typeFactory();

        return Stream.of(
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.decimalOf(2, 0)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(2, 0)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(8, 6))
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.decimalOf(5, 0)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(5, 0)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(11, 6))
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.decimalOf(5, 1)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(5, 0)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(11, 7))
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.decimalOf(5, 1)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(5, 1)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(12, 7))
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.decimalOf(5, 2)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(5, 2)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(13, 8))
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.decimalOf(5, 3)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(5, 3)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(14, 9))
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.decimalOf(10, 5)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(10, 5)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(26, 16))
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.decimalOf(10, 9)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(10, 9)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(30, 20))
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32000, 9)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32000, 0)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32767, 776))
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32000, 0)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32000, 9)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32767, 758))
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32000, 9)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32000, 9)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32767, 767))
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32767, 9)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32767, 0)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32767, 9))
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32767, 0)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32767, 150)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32767, 0))
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32767, 32767)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32767, 0)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32767, 32767))
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32767, 32767)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32767, 32765)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32767, 2))
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32767, 32767)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32767, 32667)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32767, 100))
                ),
                Arguments.of(
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32767, 32767)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32767, 32767)),
                        native2relationalType(typeFactory, NativeTypes.decimalOf(32767, 0))
                )
        );
    }

    // Precision

    @ParameterizedTest
    @MethodSource("typesWithPrecision")
    public void testCatalogMaxPrecisionCompatibility(ColumnType columnType, SqlTypeName sqlTypeName) {
        int catalogMaxPrecision = CatalogUtils.getMaxPrecision(columnType);
        int typeSystemValue = typeSystem.getMaxPrecision(sqlTypeName);
        assertEquals(catalogMaxPrecision, typeSystemValue);
    }

    @ParameterizedTest
    @MethodSource("typesWithPrecision")
    public void testCatalogMinPrecisionCompatibility(ColumnType columnType, SqlTypeName sqlTypeName) {
        int catalogValue = CatalogUtils.getMinPrecision(columnType);
        int typeSystemValue = typeSystem.getMinPrecision(sqlTypeName);
        assertEquals(catalogValue, typeSystemValue);
    }

    // Length

    @ParameterizedTest
    @MethodSource("typesWithLength")
    public void testCatalogMaxLengthCompatibility(ColumnType columnType, SqlTypeName sqlTypeName) {
        int catalogValue = CatalogUtils.getMaxLength(columnType);
        int typeSystemValue = typeSystem.getMaxPrecision(sqlTypeName);
        assertEquals(catalogValue, typeSystemValue);
    }

    @ParameterizedTest
    @MethodSource("typesWithLength")
    public void testCatalogMinLengthCompatibility(ColumnType columnType, SqlTypeName sqlTypeName) {
        int catalogValue = CatalogUtils.getMinLength(columnType);

        int typeSystemValue = typeSystem.getMinPrecision(sqlTypeName);
        assertEquals(catalogValue, typeSystemValue);
    }

    // Scale

    @ParameterizedTest
    @MethodSource("typesWithScale")
    public void testCatalogMaxScaleCompatibility(ColumnType columnType, SqlTypeName sqlTypeName) {
        int catalogValue = CatalogUtils.getMaxScale(columnType);

        int typeSystemValue = typeSystem.getMaxScale(sqlTypeName);
        assertEquals(catalogValue, typeSystemValue);
    }

    @ParameterizedTest
    @MethodSource("typesWithScale")
    public void testCatalogMinScaleCompatibility(ColumnType columnType, SqlTypeName sqlTypeName) {
        int catalogValue = CatalogUtils.getMinScale(columnType);
        int typeSystemValue = typeSystem.getMinScale(sqlTypeName);
        assertEquals(catalogValue, typeSystemValue);
    }

    private static Stream<Arguments> typesWithPrecision() {
        return Arrays.stream(ColumnType.values()).filter(ColumnType::precisionAllowed)
                .flatMap(IgniteTypeSystemTest::sqlTypesForColumnType);
    }

    private static Stream<Arguments> typesWithScale() {
        return Arrays.stream(ColumnType.values()).filter(ColumnType::scaleAllowed)
                .flatMap(IgniteTypeSystemTest::sqlTypesForColumnType);
    }

    private static Stream<Arguments> typesWithLength() {
        return Arrays.stream(ColumnType.values()).filter(ColumnType::lengthAllowed)
                .flatMap(IgniteTypeSystemTest::sqlTypesForColumnType);
    }

    private static Stream<Arguments> sqlTypesForColumnType(ColumnType columnType) {
        return columnTypeToSqlTypes(columnType).stream().map(s -> Arguments.arguments(columnType, s));
    }

    private static List<SqlTypeName> columnTypeToSqlTypes(ColumnType columnType) {
        switch (columnType) {
            case NULL:
                return List.of(SqlTypeName.NULL);
            case BOOLEAN:
                return List.of(SqlTypeName.BOOLEAN);
            case INT8:
                return List.of(SqlTypeName.TINYINT);
            case INT16:
                return List.of(SqlTypeName.SMALLINT);
            case INT32:
                return List.of(SqlTypeName.INTEGER);
            case INT64:
                return List.of(SqlTypeName.BIGINT);
            case FLOAT:
                return List.of(SqlTypeName.REAL);
            case DOUBLE:
                return List.of(SqlTypeName.DOUBLE);
            case DECIMAL:
                return List.of(SqlTypeName.DECIMAL);
            case DATE:
                return List.of(SqlTypeName.DATE);
            case TIME:
                return List.of(SqlTypeName.TIME);
            case DATETIME:
                return List.of(SqlTypeName.TIMESTAMP);
            case TIMESTAMP:
                return List.of(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
            case STRING:
                return List.of(SqlTypeName.CHAR, SqlTypeName.VARCHAR);
            case BYTE_ARRAY:
                return List.of(SqlTypeName.BINARY, SqlTypeName.VARBINARY);
            case PERIOD:
                return List.of(
                        SqlTypeName.INTERVAL_YEAR,
                        SqlTypeName.INTERVAL_YEAR_MONTH,
                        SqlTypeName.INTERVAL_MONTH
                );
            case DURATION:
                return List.of(
                        SqlTypeName.INTERVAL_DAY,
                        SqlTypeName.INTERVAL_DAY_HOUR,
                        SqlTypeName.INTERVAL_DAY_MINUTE,
                        SqlTypeName.INTERVAL_DAY_SECOND,
                        SqlTypeName.INTERVAL_HOUR,
                        SqlTypeName.INTERVAL_HOUR_MINUTE,
                        SqlTypeName.INTERVAL_HOUR_SECOND,
                        SqlTypeName.INTERVAL_MINUTE,
                        SqlTypeName.INTERVAL_MINUTE_SECOND,
                        SqlTypeName.INTERVAL_SECOND
                );
            default:
                throw new IllegalArgumentException("Unexpected type: " + columnType);
        }
    }
}
