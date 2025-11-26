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

package org.apache.ignite.internal.sql.engine.exec;

import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactoryImpl;
import org.apache.ignite.internal.sql.engine.framework.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Test;

/**
 * RuntimeTreeIndexTest.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class RuntimeSortedIndexTest extends IgniteAbstractTest {
    private static final int UNIQUE_GROUPS = 10_000;

    private static final int[] NOT_UNIQUE_ROWS_IN_GROUP = {1, 10};

    private static final Pair<NativeType[], ImmutableIntList>[] ROW_TYPES = new Pair[]{
            new Pair<>(new NativeType[]{NativeTypes.INT32, NativeTypes.INT32, NativeTypes.INT32}, ImmutableIntList.of(1)),

            new Pair<>(new NativeType[]{NativeTypes.INT32, NativeTypes.INT64, NativeTypes.INT32}, ImmutableIntList.of(1)),
            new Pair<>(new NativeType[]{NativeTypes.INT32, NativeTypes.STRING, NativeTypes.INT32}, ImmutableIntList.of(1)),
            new Pair<>(new NativeType[]{NativeTypes.INT32, NativeTypes.DATE, NativeTypes.INT32}, ImmutableIntList.of(1)),
            new Pair<>(new NativeType[]{NativeTypes.INT32, NativeTypes.time(0), NativeTypes.INT32}, ImmutableIntList.of(1)),
            new Pair<>(new NativeType[]{NativeTypes.INT32, NativeTypes.datetime(6), NativeTypes.INT32}, ImmutableIntList.of(1)),
            new Pair<>(new NativeType[]{NativeTypes.INT32, NativeTypes.timestamp(6), NativeTypes.INT32}, ImmutableIntList.of(1)),
            new Pair<>(new NativeType[]{NativeTypes.INT32, NativeTypes.STRING, NativeTypes.time(0),
                    NativeTypes.DATE, NativeTypes.datetime(6), NativeTypes.INT32}, ImmutableIntList.of(1, 2, 3, 4))
    };

    /** Search count. */
    private static final int SEARCH_CNT = UNIQUE_GROUPS / 100;

    @Test
    public void test() throws Exception {
        IgniteTypeFactory tf = Commons.typeFactory();

        List<Pair<RelDataType, ImmutableIntList>> testIndexes = Arrays.stream(ROW_TYPES)
                .map(rt -> Pair.of(TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, rt.getKey())), rt.getValue()))
                .collect(Collectors.toList());

        for (Pair<RelDataType, ImmutableIntList> testIdx : testIndexes) {
            for (int notUnique : NOT_UNIQUE_ROWS_IN_GROUP) {
                RuntimeSortedIndex<Object[]> idx0 = generate(testIdx.getKey(), testIdx.getValue(), notUnique);

                int rowIdLow = ThreadLocalRandom.current().nextInt(UNIQUE_GROUPS * notUnique);
                int rowIdUp = rowIdLow + ThreadLocalRandom.current().nextInt(UNIQUE_GROUPS * notUnique - rowIdLow);

                for (int searchNum = 0; searchNum < SEARCH_CNT; ++searchNum) {
                    Object[] lower = generateFindRow(rowIdLow, testIdx.getKey(), notUnique, testIdx.getValue());
                    Object[] upper = generateFindRow(rowIdUp, testIdx.getKey(), notUnique, testIdx.getValue());

                    Cursor<Object[]> cur = idx0.find(lower, upper, true, true);

                    int rows = 0;
                    while (cur.hasNext()) {
                        cur.next();

                        rows++;
                    }

                    assertEquals(
                            (rowIdUp / notUnique - rowIdLow / notUnique + 1) * notUnique,
                            rows,
                            "Invalid results [rowType=" + testIdx.getKey() + ", notUnique=" + notUnique
                                    + ", rowIdLow=" + rowIdLow + ", rowIdUp=" + rowIdUp
                    );
                }
            }
        }
    }

    private RuntimeSortedIndex<Object[]> generate(RelDataType rowType, final List<Integer> idxCols, int notUnique) {
        ClusterNodeImpl node = new ClusterNodeImpl(randomUUID(), "fake-test-node", NetworkAddress.from("127.0.0.1:1111"));
        RuntimeSortedIndex<Object[]> idx = new RuntimeSortedIndex<>(
                new ExecutionContext<>(
                        new ExpressionFactoryImpl<>(Commons.typeFactory(), 1024, CaffeineCacheFactory.INSTANCE),
                        null,
                        new ExecutionId(randomUUID(), 0),
                        node,
                        node.name(),
                        node.id(),
                        null,
                        ArrayRowHandler.INSTANCE,
                        Map.of(),
                        null,
                        SqlCommon.DEFAULT_TIME_ZONE_ID,
                        -1,
                        Clock.systemUTC(),
                        null,
                        1L
                ),
                RelCollations.of(ImmutableIntList.copyOf(idxCols)),
                (o1, o2) -> {
                    for (int colIdx : idxCols) {
                        int res = ((Comparable) o1[colIdx]).compareTo(o2[colIdx]);

                        if (res != 0) {
                            return res;
                        }
                    }

                    return 0;
                });

        BitSet rowIds = new BitSet(UNIQUE_GROUPS);

        // First random fill
        for (int i = 0; i < UNIQUE_GROUPS * notUnique; ++i) {
            if (!rowIds.get(i)) {
                idx.push(generateRow(i, rowType, notUnique));
                rowIds.set(i);
            }
        }

        for (int i = 0; i < UNIQUE_GROUPS * notUnique; ++i) {
            if (!rowIds.get(i)) {
                idx.push(generateRow(i, rowType, notUnique));
            }
        }

        return idx;
    }

    private Object[] generateRow(int rowId, RelDataType rowType, int notUnique) {
        Object[] row = new Object[rowType.getFieldCount()];

        for (int i = 0; i < rowType.getFieldCount(); ++i) {
            row[i] = generateValue(rowId, rowType.getFieldList().get(i), notUnique);
        }

        return row;
    }

    private Object[] generateFindRow(int rowId, RelDataType rowType, int notUnique, final List<Integer> idxCols) {
        Object[] row = generateRow(rowId, rowType, notUnique);

        for (int i = 0; i < rowType.getFieldCount(); ++i) {
            if (!idxCols.contains(i)) {
                row[i] = null;
            }
        }

        return row;
    }

    private Object generateValue(int rowId, RelDataTypeField field, int notUnique) {
        long mod = rowId / notUnique;
        long baseDate = 1_000_000L;

        switch (field.getType().getSqlTypeName().getFamily()) {
            case NUMERIC:
                return mod;

            case CHARACTER:
                return "val " + String.format("%07d", mod);

            case DATE:
                return new Date(baseDate + mod * 10_000);

            case TIME:
                return new Time(mod);

            case TIMESTAMP:
                return new Timestamp(baseDate + mod);

            default:
                assert false : "Not supported type for test: " + field.getType();
                return null;
        }
    }
}
