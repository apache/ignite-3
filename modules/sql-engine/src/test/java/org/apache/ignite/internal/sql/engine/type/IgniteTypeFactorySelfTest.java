package org.apache.ignite.internal.sql.engine.type;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link IgniteTypeFactory}.
 */
public class IgniteTypeFactorySelfTest extends BaseIgniteAbstractTest {

    private static final IgniteTypeFactory TYPE_FACTORY = Commons.typeFactory();

    private final Random random = new Random();

    @BeforeEach
    public void test() {
        long seed = System.nanoTime();
        random.setSeed(seed);
        log.info("Seed: {}", seed);
    }

    @ParameterizedTest
    @MethodSource("types")
    public void leastRestrictiveTypeBetweenTimestampsTypes(List<RelDataType> types, RelDataType expected) {
        RelDataType actual = TYPE_FACTORY.leastRestrictive(types);

        List<RelDataType> shuffledTypes = new ArrayList<>(types);
        Collections.shuffle(shuffledTypes, random);

        assertEquals(expected, actual);
    }

    private static Stream<Arguments> types() {
        return Stream.of(
                // timestamp

                Arguments.of(List.of(
                                timestamp(1)
                        ),
                        timestamp(1)),

                Arguments.of(List.of(
                                timestamp(1),
                                timestamp(3)
                        ),
                        timestamp(3)),

                Arguments.of(List.of(
                                timestamp(1),
                                timestamp(3),
                                timestamp(2)
                        ),
                        timestamp(3)),

                Arguments.of(List.of(
                                timestamp(),
                                timestamp(3)
                        ),
                        timestamp(6)),

                // timestamp ltz

                Arguments.of(List.of(
                                timestampLtz(1)
                        ),
                        timestampLtz(1)),

                Arguments.of(List.of(
                                timestampLtz(1),
                                timestampLtz(3)
                        ),
                        timestampLtz(3)),

                Arguments.of(List.of(
                                timestampLtz(1),
                                timestampLtz(3),
                                timestampLtz(2)
                        ),
                        timestampLtz(3)),

                Arguments.of(List.of(
                                timestampLtz(),
                                timestampLtz(3)
                        ),
                        timestampLtz(6)),

                // timestamp ltz v timestamp

                Arguments.of(List.of(
                                timestamp(1),
                                timestampLtz(3)
                        ),
                        timestamp(3)),

                Arguments.of(List.of(
                                timestampLtz(3),
                                timestamp(1)
                        ),
                        timestamp(3)),

                Arguments.of(List.of(
                                timestampLtz(1),
                                timestampLtz(3),
                                timestamp(2)
                        ),
                        timestamp(3)),

                // other
                Arguments.of(List.of(
                                timestamp(1),
                                timestamp(3),
                                TYPE_FACTORY.createSqlType(SqlTypeName.DATE)
                        ),
                        null),

                Arguments.of(List.of(
                                timestampLtz(1),
                                timestampLtz(3),
                                TYPE_FACTORY.createSqlType(SqlTypeName.DATE)
                        ),
                        null)
        );
    }

    private static RelDataType timestamp(int p) {
        return TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP, p);
    }

    private static RelDataType timestamp() {
        return TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP);
    }

    private static RelDataType timestampLtz(int p) {
        return TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, p);
    }

    private static RelDataType timestampLtz() {
        return TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    }
}
