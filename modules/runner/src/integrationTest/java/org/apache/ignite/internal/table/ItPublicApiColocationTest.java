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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
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
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
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
    public void colocationOneColumn(NativeTypeSpec type) {
        String sqlType = SqlTestUtils.toSqlType(type.asColumnType());
        sql(String.format("create table test0(id %s primary key, v INTEGER)", sqlType));
        sql(String.format("create table test1(id0 integer, id1 %s, v INTEGER, primary key(id0, id1)) colocate by(id1)", sqlType));

        int rowsCnt = type == NativeTypeSpec.BOOLEAN ? 2 : ROWS;

        for (int i = 0; i < rowsCnt; ++i) {
            Object val = SqlTestUtils.generateStableValueByType(i, type);
            sql("insert into test0 values(?, ?)", val, 0);
            sql("insert into test1 values(?, ?, ?)", i, val, 0);
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
    public void colocationTwoColumns(NativeTypeSpec t0, NativeTypeSpec t1) {
        String sqlType0 = SqlTestUtils.toSqlType(t0.asColumnType());
        String sqlType1 = SqlTestUtils.toSqlType(t1.asColumnType());
        sql(String.format("create table test0(id0 %s, id1 %s, v INTEGER, primary key(id0, id1))", sqlType0, sqlType1));

        sql(String.format(
                "create table test1(id integer, id0 %s, id1 %s, v INTEGER, primary key(id, id0, id1)) colocate by(id0, id1)",
                sqlType0,
                sqlType1
        ));

        int rowsCnt = (t0 == NativeTypeSpec.BOOLEAN || t1 == NativeTypeSpec.BOOLEAN) ? 2 : ROWS;

        for (int i = 0; i < rowsCnt; ++i) {
            Object val1 = SqlTestUtils.generateStableValueByType(i, t0);
            Object val2 = SqlTestUtils.generateStableValueByType(i, t1);
            sql("insert into test0 values(?, ?, ?)", val1, val2, 0);
            sql("insert into test1 values(?, ?, ?, ?)", i, val1, val2, 0);
        }

        int parts = unwrapTableViewInternal(CLUSTER.aliveNode().tables().table("test0")).internalTable().partitions();
        TableViewInternal tbl0 = unwrapTableViewInternal(CLUSTER.aliveNode().tables().table("test0"));
        TableViewInternal tbl1 = unwrapTableViewInternal(CLUSTER.aliveNode().tables().table("test1"));

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
                args.add(Arguments.of(t0, t1));
            }
        }

        return args.stream();
    }

    private static Stream<Arguments> oneColumnParameters() {
        List<Arguments> args = new ArrayList<>();

        for (NativeTypeSpec t : NativeTypeSpec.values()) {
            args.add(Arguments.of(t));
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
}
