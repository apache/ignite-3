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

import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for {@link IgniteTypeSystem}. */
public class IgniteTypeSystemTest extends BaseIgniteAbstractTest {

    private static final int TIME_PRECISION = 9;
    private static final int STRING_PRECISION = 65536;
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
        assertEquals(TIME_PRECISION, typeSystem.getMaxPrecision(SqlTypeName.TIME));
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

    @ParameterizedTest(name = "op={0} {3} {1}")
    @MethodSource("deriveDivTypeArguments")
    void deriveDivType(NativeType type1, NativeType type2, NativeType expected, ArithmeticOp op) {
        RelDataType relType1 = native2relationalType(Commons.typeFactory(), type1);
        RelDataType relType2 = native2relationalType(Commons.typeFactory(), type2);
        RelDataType relExpected = native2relationalType(Commons.typeFactory(), expected);

        RelDataType actual = typeSystem.deriveDecimalDivideType(Commons.typeFactory(), relType1, relType2);
        assertThat(actual, Matchers.equalTo(relExpected));
    }

    @ParameterizedTest(name = "op={0} {3} {1}")
    @MethodSource("deriveMultTypeArguments")
    void deriveMultType(NativeType type1, NativeType type2, NativeType expected, ArithmeticOp op) {
        RelDataType relType1 = native2relationalType(Commons.typeFactory(), type1);
        RelDataType relType2 = native2relationalType(Commons.typeFactory(), type2);
        RelDataType relExpected = native2relationalType(Commons.typeFactory(), expected);

        RelDataType actual = typeSystem.deriveDecimalMultiplyType(Commons.typeFactory(), relType1, relType2);
        assertThat(actual, Matchers.equalTo(relExpected));
    }

    @ParameterizedTest(name = "op={0} {3} {1}")
    @MethodSource("deriveAddTypeArguments")
    void deriveAddType(NativeType type1, NativeType type2, NativeType expected, ArithmeticOp op) {
        RelDataType relType1 = native2relationalType(Commons.typeFactory(), type1);
        RelDataType relType2 = native2relationalType(Commons.typeFactory(), type2);
        RelDataType relExpected = native2relationalType(Commons.typeFactory(), expected);

        RelDataType actual = typeSystem.deriveDecimalPlusType(Commons.typeFactory(), relType1, relType2);
        assertThat(actual, Matchers.equalTo(relExpected));
    }

    private static Stream<Arguments> deriveDivTypeArguments() {
        IgniteTypeSystem typeSystem = IgniteTypeSystem.INSTANCE;

        return Stream.of(
                Arguments.of(NativeTypes.INT8, NativeTypes.INT8, NativeTypes.INT16, ArithmeticOp.DIV),
                Arguments.of(NativeTypes.INT8, NativeTypes.INT16, NativeTypes.INT16, ArithmeticOp.DIV),
                Arguments.of(NativeTypes.INT8, NativeTypes.INT32, NativeTypes.INT16, ArithmeticOp.DIV),
                Arguments.of(NativeTypes.INT8, NativeTypes.INT64, NativeTypes.INT16, ArithmeticOp.DIV),

                Arguments.of(NativeTypes.INT16, NativeTypes.INT8, NativeTypes.INT32, ArithmeticOp.DIV),
                Arguments.of(NativeTypes.INT16, NativeTypes.INT16, NativeTypes.INT32, ArithmeticOp.DIV),
                Arguments.of(NativeTypes.INT16, NativeTypes.INT32, NativeTypes.INT32, ArithmeticOp.DIV),
                Arguments.of(NativeTypes.INT16, NativeTypes.INT64, NativeTypes.INT32, ArithmeticOp.DIV),

                Arguments.of(NativeTypes.INT32, NativeTypes.INT8, NativeTypes.INT64, ArithmeticOp.DIV),
                Arguments.of(NativeTypes.INT32, NativeTypes.INT16, NativeTypes.INT64, ArithmeticOp.DIV),
                Arguments.of(NativeTypes.INT32, NativeTypes.INT32, NativeTypes.INT64, ArithmeticOp.DIV),
                Arguments.of(NativeTypes.INT32, NativeTypes.INT64, NativeTypes.INT64, ArithmeticOp.DIV),

                Arguments.of(NativeTypes.INT64, NativeTypes.INT8, NativeTypes.decimalOf(typeSystem.getMaxPrecision(SqlTypeName.DECIMAL), 0),
                        ArithmeticOp.DIV),
                Arguments.of(NativeTypes.INT64, NativeTypes.INT16, NativeTypes.decimalOf(typeSystem.getMaxPrecision(SqlTypeName.DECIMAL), 0),
                        ArithmeticOp.DIV),
                Arguments.of(NativeTypes.INT64, NativeTypes.INT32, NativeTypes.decimalOf(typeSystem.getMaxPrecision(SqlTypeName.DECIMAL), 0),
                        ArithmeticOp.DIV),
                Arguments.of(NativeTypes.INT64, NativeTypes.INT64, NativeTypes.decimalOf(typeSystem.getMaxPrecision(SqlTypeName.DECIMAL), 0),
                        ArithmeticOp.DIV)
        );
    }


    private static Stream<Arguments> deriveMultTypeArguments() {
        IgniteTypeSystem typeSystem = IgniteTypeSystem.INSTANCE;

        return Stream.of(
                Arguments.of(NativeTypes.INT8, NativeTypes.INT8, NativeTypes.INT16, ArithmeticOp.MULT),
                Arguments.of(NativeTypes.INT8, NativeTypes.INT16, NativeTypes.INT32, ArithmeticOp.MULT),
                Arguments.of(NativeTypes.INT8, NativeTypes.INT32, NativeTypes.INT64, ArithmeticOp.MULT),
                Arguments.of(NativeTypes.INT8, NativeTypes.INT64, NativeTypes.decimalOf(typeSystem.getMaxPrecision(SqlTypeName.DECIMAL), 0),
                        ArithmeticOp.MULT),

                Arguments.of(NativeTypes.INT16, NativeTypes.INT8, NativeTypes.INT32, ArithmeticOp.MULT),
                Arguments.of(NativeTypes.INT16, NativeTypes.INT16, NativeTypes.INT32, ArithmeticOp.MULT),
                Arguments.of(NativeTypes.INT16, NativeTypes.INT32, NativeTypes.INT64, ArithmeticOp.MULT),
                Arguments.of(NativeTypes.INT16, NativeTypes.INT64, NativeTypes.decimalOf(typeSystem.getMaxPrecision(SqlTypeName.DECIMAL), 0),
                        ArithmeticOp.MULT),

                Arguments.of(NativeTypes.INT32, NativeTypes.INT8, NativeTypes.INT64, ArithmeticOp.MULT),
                Arguments.of(NativeTypes.INT32, NativeTypes.INT16, NativeTypes.INT64, ArithmeticOp.MULT),
                Arguments.of(NativeTypes.INT32, NativeTypes.INT32, NativeTypes.INT64, ArithmeticOp.MULT),
                Arguments.of(NativeTypes.INT32, NativeTypes.INT64, NativeTypes.decimalOf(typeSystem.getMaxPrecision(SqlTypeName.DECIMAL), 0),
                        ArithmeticOp.MULT),

                Arguments.of(NativeTypes.INT64, NativeTypes.INT8, NativeTypes.decimalOf(typeSystem.getMaxPrecision(SqlTypeName.DECIMAL), 0),
                        ArithmeticOp.MULT),
                Arguments.of(NativeTypes.INT64, NativeTypes.INT16, NativeTypes.decimalOf(typeSystem.getMaxPrecision(SqlTypeName.DECIMAL), 0),
                        ArithmeticOp.MULT),
                Arguments.of(NativeTypes.INT64, NativeTypes.INT32, NativeTypes.decimalOf(typeSystem.getMaxPrecision(SqlTypeName.DECIMAL), 0),
                        ArithmeticOp.MULT),
                Arguments.of(NativeTypes.INT64, NativeTypes.INT64, NativeTypes.decimalOf(typeSystem.getMaxPrecision(SqlTypeName.DECIMAL), 0),
                        ArithmeticOp.MULT)
        );
    }

    private static Stream<Arguments> deriveAddTypeArguments() {
        IgniteTypeSystem typeSystem = IgniteTypeSystem.INSTANCE;

        return Stream.of(
                Arguments.of(NativeTypes.INT8, NativeTypes.INT8, NativeTypes.INT16, ArithmeticOp.ADD),
                Arguments.of(NativeTypes.INT8, NativeTypes.INT16, NativeTypes.INT32, ArithmeticOp.ADD),
                Arguments.of(NativeTypes.INT8, NativeTypes.INT32, NativeTypes.INT64, ArithmeticOp.ADD),
                Arguments.of(NativeTypes.INT8, NativeTypes.INT64, NativeTypes.decimalOf(typeSystem.getMaxPrecision(SqlTypeName.DECIMAL), 0),
                        ArithmeticOp.ADD),

                Arguments.of(NativeTypes.INT16, NativeTypes.INT8, NativeTypes.INT32, ArithmeticOp.ADD),
                Arguments.of(NativeTypes.INT16, NativeTypes.INT16, NativeTypes.INT32, ArithmeticOp.ADD),
                Arguments.of(NativeTypes.INT16, NativeTypes.INT32, NativeTypes.INT64, ArithmeticOp.ADD),
                Arguments.of(NativeTypes.INT16, NativeTypes.INT64, NativeTypes.decimalOf(typeSystem.getMaxPrecision(SqlTypeName.DECIMAL), 0),
                        ArithmeticOp.ADD),

                Arguments.of(NativeTypes.INT32, NativeTypes.INT8, NativeTypes.INT64, ArithmeticOp.ADD),
                Arguments.of(NativeTypes.INT32, NativeTypes.INT16, NativeTypes.INT64, ArithmeticOp.ADD),
                Arguments.of(NativeTypes.INT32, NativeTypes.INT32, NativeTypes.INT64, ArithmeticOp.ADD),
                Arguments.of(NativeTypes.INT32, NativeTypes.INT64, NativeTypes.decimalOf(typeSystem.getMaxPrecision(SqlTypeName.DECIMAL), 0),
                        ArithmeticOp.ADD),

                Arguments.of(NativeTypes.INT64, NativeTypes.INT8, NativeTypes.decimalOf(typeSystem.getMaxPrecision(SqlTypeName.DECIMAL), 0),
                        ArithmeticOp.ADD),
                Arguments.of(NativeTypes.INT64, NativeTypes.INT16, NativeTypes.decimalOf(typeSystem.getMaxPrecision(SqlTypeName.DECIMAL), 0),
                        ArithmeticOp.ADD),
                Arguments.of(NativeTypes.INT64, NativeTypes.INT32, NativeTypes.decimalOf(typeSystem.getMaxPrecision(SqlTypeName.DECIMAL), 0),
                        ArithmeticOp.ADD),
                Arguments.of(NativeTypes.INT64, NativeTypes.INT64, NativeTypes.decimalOf(typeSystem.getMaxPrecision(SqlTypeName.DECIMAL), 0),
                        ArithmeticOp.ADD)
        );
    }

    private enum ArithmeticOp {
        ADD, MULT, DIV
    }
}
