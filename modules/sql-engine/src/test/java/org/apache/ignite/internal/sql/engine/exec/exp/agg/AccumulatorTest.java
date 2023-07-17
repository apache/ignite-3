package org.apache.ignite.internal.sql.engine.exec.exp.agg;

import static org.apache.ignite.lang.IgniteStringFormatter.format;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;

public class AccumulatorTest {

    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = {"NULL", "PERIOD", "DURATION", "BITMASK", "BOOLEAN"}, mode = Mode.EXCLUDE)
    public void testSum(ColumnType columnType) {
        Tester tester = new Tester(columnType, SqlStdOperatorTable.SUM);

        tester.init();
        tester.expectState(true, 0L);

        tester.update(1L);
        tester.expectState(false, 1L);

        tester.update(2L);
        tester.expectState(false, 3L);

        tester.update(-4L);
        tester.expectState(false, -1L);
    }

    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = {"NULL", "PERIOD", "DURATION", "BITMASK", "BOOLEAN"}, mode = Mode.EXCLUDE)
    public void testCount(ColumnType columnType) {
        Tester tester = new Tester(columnType, SqlStdOperatorTable.COUNT);

        tester.init();
        tester.expectState(0L);

        int any = 1;

        tester.update(any);
        tester.expectState(1L);

        tester.update(any);
        tester.expectState(2L);

        tester.update(any);
        tester.expectState(3L);
    }

    @ParameterizedTest
    @EnumSource(value = ColumnType.class, names = {"NULL", "PERIOD", "DURATION", "BITMASK", "BOOLEAN"}, mode = Mode.EXCLUDE)
    public void testAny(ColumnType columnType) {
        Tester tester = new Tester(columnType, SqlStdOperatorTable.ANY_VALUE);

        tester.init();
        tester.expectState(new Object[]{null});

        int any1 = 2;
        int any2 = 3;

        tester.update(any1);
        tester.expectState(2);

        tester.update(any2);
        tester.expectState(2);
    }

    private static class Tester {

        private final Object[] data = new Object[10];

        private final AccState state;

        private final Accumulator accumulator;

        Tester(ColumnType typeName, SqlAggFunction function) {
            IgniteTypeFactory typeFactory = Commons.typeFactory();

            NativeType nativeType = TypeUtils.columnType2NativeType(typeName, 2, 2);
            RelDataType relDataType = TypeUtils.native2relationalType(typeFactory, nativeType, false);

            RelDataType dataType = new RelDataTypeFactory.Builder(typeFactory)
                    .add("field", relDataType)
                    .build();

            AggregateCall call = AggregateCall.create(function, false, false, false, List.of(0), -1, null,
                    RelCollations.EMPTY,  dataType, null);

            Supplier<Accumulator> supplier = new Accumulators(typeFactory).accumulatorFactory(call);

            accumulator = supplier.get();

            state = new AccState(data);
        }

        void init() {
            accumulator.writeState(state);
        }

        void update(Object val) {
            state.reset();

            accumulator.add(val);
        }

        void expectState(Object... expectedState) {
            state.reset();

            accumulator.writeState(state);

            Object[] actualState = new Object[expectedState.length];
            System.arraycopy(data, 0, actualState, 0, expectedState.length);

            String message = format("{}: Actual state: {}, Expected: {}",
                    accumulator.getClass(), Arrays.toString(actualState), Arrays.toString(expectedState));

            assertArrayEquals(actualState, expectedState, message);
        }
    }
}
