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

package org.apache.ignite.internal.table;

import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for the data colocation.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItPublicApiColocationTest extends ClusterPerClassIntegrationTest {
    /** Rows count ot test. */
    private static final int ROWS = 10;

    /**
     * Excluded native types.
     */
    private static final Set<NativeTypeSpec> EXCLUDED_TYPES = Stream.of(
            NativeTypeSpec.BITMASK,
            NativeTypeSpec.NUMBER)
            .collect(Collectors.toSet());

    @AfterEach
    public void dropTables() {
        for (Table t : CLUSTER.aliveNode().tables().tables()) {
            sql("DROP TABLE " + t.name());
        }
    }

    /**
     * Check colocation by one column PK and explicit colocation key for all types.
     */
    @ParameterizedTest(name = "type=" + ARGUMENTS_PLACEHOLDER)
    @MethodSource("oneColumnParameters")
    public void colocationOneColumn(NativeTypeSpec type) throws Exception {
        sql(String.format("create table test0(id %s primary key, v INTEGER)", sqlTypeName(type)));
        sql(String.format("create table test1(id0 integer, id1 %s, v INTEGER, primary key(id0, id1)) colocate by(id1)", sqlTypeName(type)));

        int rowsCnt = type == NativeTypeSpec.BOOLEAN ? 2 : ROWS;

        for (int i = 0; i < rowsCnt; ++i) {
            sql("insert into test0 values(?, ?)", generateValueByType(i, type), 0);
            sql("insert into test1 values(?, ?, ?)", i, generateValueByType(i, type), 0);
        }

        TableViewInternal tableViewInternal = unwrapTableViewInternal(CLUSTER.aliveNode().tables().table("test0"));
        int parts = tableViewInternal.internalTable().partitions();
        TableViewInternal tbl0 = unwrapTableViewInternal(CLUSTER.aliveNode().tables().table("test0"));
        TableViewInternal tbl1 = unwrapTableViewInternal(CLUSTER.aliveNode().tables().table("test1"));

        for (int i = 0; i < parts; ++i) {
            List<Tuple> r0 = getAllBypassingThreadAssertions(tbl0, i);

            Set<Object> ids0 = r0.stream().map(t -> t.value("id")).collect(Collectors.toSet());
            List<Tuple> r1 = getAllBypassingThreadAssertions(tbl1, i);

            // because the byte array is not comparable, we need to check the type separately
            if (type == NativeTypeSpec.BYTES) {
                r1.forEach(t -> {
                    byte[] k = t.value("id1");
                    ids0.stream().filter(v -> Arrays.equals((byte[]) v, k)).findAny().ifPresent(ids0::remove);
                });
            } else {
                r1.forEach(t -> assertTrue(ids0.remove(t.value("id1"))));
            }

            assertTrue(ids0.isEmpty());
        }
    }

    /**
     * Check colocation by one column for all types.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-17557")
    @ParameterizedTest(name = "types=" + ARGUMENTS_PLACEHOLDER)
    @MethodSource("twoColumnsParameters")
    public void colocationTwoColumns(NativeTypeSpec t0, NativeTypeSpec t1) throws Exception {
        sql(String.format("create table test0(id0 %s, id1 %s, v INTEGER, primary key(id0, id1))", sqlTypeName(t0), sqlTypeName(t1)));

        sql(String.format(
                "create table test1(id integer, id0 %s, id1 %s, v INTEGER, primary key(id, id0, id1)) colocate by(id0, id1)",
                sqlTypeName(t0),
                sqlTypeName(t1)
        ));

        for (int i = 0; i < ROWS; ++i) {
            sql("insert into test0 values(?, ?, ?)", generateValueByType(i, t0), generateValueByType(i, t1), 0);
            sql("insert into test1 values(?, ?, ?, ?)", i, generateValueByType(i, t0), generateValueByType(i, t1), 0);
        }

        int parts = ((TableViewInternal) CLUSTER.aliveNode().tables().table("test0")).internalTable().partitions();
        TableViewInternal tbl0 = (TableViewInternal) CLUSTER.aliveNode().tables().table("test0");
        TableViewInternal tbl1 = (TableViewInternal) CLUSTER.aliveNode().tables().table("test1");

        Function<Tuple, Tuple> tupleColocationExtract = (t) -> {
            Tuple ret = Tuple.create();
            ret.set("id0", t.value("id0"));
            ret.set("id1", t.value("id1"));
            return ret;
        };

        for (int i = 0; i < parts; ++i) {
            List<Tuple> r0 = getAllBypassingThreadAssertions(tbl0, i);

            Set<Tuple> ids0 = r0.stream().map(tupleColocationExtract).collect(Collectors.toSet());

            List<Tuple> r1 = getAllBypassingThreadAssertions(tbl1, i);

            r1.forEach(t -> assertTrue(ids0.remove(tupleColocationExtract.apply(t))));

            assertTrue(ids0.isEmpty());
        }
    }

    private static Stream<Arguments> twoColumnsParameters() {
        List<Arguments> args = new ArrayList<>();

        for (NativeTypeSpec t0 : NativeTypeSpec.values()) {
            for (NativeTypeSpec t1 : NativeTypeSpec.values()) {
                if (!EXCLUDED_TYPES.contains(t0) && !EXCLUDED_TYPES.contains(t1)) {
                    args.add(Arguments.of(t0, t1));
                }
            }
        }

        return args.stream();
    }

    private static Stream<Arguments> oneColumnParameters() {
        List<Arguments> args = new ArrayList<>();

        for (NativeTypeSpec t : NativeTypeSpec.values()) {
            if (!EXCLUDED_TYPES.contains(t)) {
                args.add(Arguments.of(t));
            }
        }

        return args.stream();
    }

    private static List<Tuple> getAllBypassingThreadAssertions(TableViewInternal tbl, int part) {
        return IgniteTestUtils.bypassingThreadAssertions(() -> {
            try {
                return getAll(tbl, part);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static List<Tuple> getAll(TableViewInternal tbl, int part) throws ExecutionException, InterruptedException {
        List<Tuple> res = new ArrayList<>();
        CompletableFuture<Void> f = new CompletableFuture<>();

        tbl.internalTable().scan(part, null).subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.request(ROWS);
                // subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(BinaryRow item) {
                SchemaRegistry registry = tbl.schemaView();
                res.add(TableRow.tuple(registry.resolve(item, registry.lastKnownSchemaVersion())));
            }

            @Override
            public void onError(Throwable throwable) {
                f.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                f.complete(null);
            }
        });

        f.get();

        return res;
    }

    static Object generateValueByType(int i, NativeTypeSpec type) {
        switch (type) {
            case BOOLEAN:
                return i % 2 == 0;
            case INT8:
                return (byte) i;
            case INT16:
                return (short) i;
            case INT32:
                return i;
            case INT64:
                return (long) i;
            case FLOAT:
                return (float) i + ((float) i / 1000);
            case DOUBLE:
                return (double) i + ((double) i / 1000);
            case DECIMAL:
                return BigDecimal.valueOf((double) i + ((double) i / 1000));
            case UUID:
                return new UUID(i, i);
            case STRING:
                return "str_" + i;
            case BYTES:
                return new byte[]{(byte) i, (byte) (i + 1), (byte) (i + 2)};
            case BITMASK:
                return BitSet.valueOf(new byte[]{(byte) i});
            case NUMBER:
                return BigInteger.valueOf(i);
            case DATE:
                return LocalDate.of(2022, 01, 01).plusDays(i);
            case TIME:
                return LocalTime.of(0, 00, 00).plusSeconds(i).plusNanos(i * 101L);
            case DATETIME:
                return LocalDateTime.of(
                        (LocalDate) generateValueByType(i, NativeTypeSpec.DATE),
                        (LocalTime) generateValueByType(i, NativeTypeSpec.TIME)
                );
            case TIMESTAMP:
                return Instant.ofEpochSecond(i * 201L, i * 101L);
            default:
                throw new IllegalStateException("Unexpected type: " + type);
        }
    }

    private static String sqlTypeName(NativeTypeSpec type) {
        switch (type) {
            case BOOLEAN:
                return "boolean";
            case INT8:
                return "tinyint";
            case INT16:
                return "smallint";
            case INT32:
                return "integer";
            case INT64:
                return "bigint";
            case FLOAT:
                return "real";
            case DOUBLE:
                return "double";
            case DECIMAL:
                return "decimal";
            case UUID:
                return "uuid";
            case STRING:
                return "varchar";
            case BYTES:
                return "varbinary";
            case BITMASK:
                return "bitmap";
            case NUMBER:
                return "number";
            case DATE:
                return "date";
            case TIME:
                return "time";
            case DATETIME:
                return "timestamp";
            case TIMESTAMP:
                return "timestamp with local time zone";
            default:
                throw new IllegalStateException("Unexpected type: " + type);
        }
    }
}
