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

package org.apache.ignite.internal.runner.app.client;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.catalog.definitions.ColumnDefinition.column;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test the OOTB support for Tuples in compute api.
 */
@SuppressWarnings("resource")
public class ItThinClientTupleComputeMarshallingTest extends ItAbstractThinClientTest {
    private static final String TABLE_NAME = "test";

    @BeforeEach
    void setUp() {
        client().catalog().createTable(
                TableDefinition.builder(TABLE_NAME)
                        .columns(
                                column("key_col", ColumnType.INT32),
                                column("value_col", ColumnType.VARCHAR)
                        ).primaryKey("key_col")
                        .build()
        );

        client().tables().table(TABLE_NAME).keyValueView().put(
                null,
                Tuple.create().set("key_col", 2),
                Tuple.create().set("value_col", "hi")
        );
    }

    @AfterEach
    void tearDown() {
        client().catalog().dropTable(TABLE_NAME);
    }

    @Test
    void tupleFromTableApiAsArgument() {
        // Given tuple from the table.
        var tup = client().tables().table(TABLE_NAME).keyValueView().get(null, Tuple.create().set("key_col", 2));

        // When execute job with the tuple as an argument.
        String result = client().compute().execute(
                JobTarget.node(node(1)),
                JobDescriptor.builder(TupleArgJob.class).build(),
                tup
        );

        // Then the result is taken from the table.
        assertThat(result, is("hi"));
    }

    @Test
    void tupleFromTableReturned() {
        // Given.
        var key = 2;

        // When execute job that returns tuple from the table.
        Tuple result = client().compute().execute(
                JobTarget.node(node(1)),
                JobDescriptor.builder(TupleResultJob.class).build(),
                key
        );

        // Then tuple is returned.
        assertThat(result.stringValue("value_col"), is("hi"));
    }

    /**
     * Tests that the nested tuples are correctly serialized and deserialized.
     */
    @Test
    void nestedTuplesArgumentSerialization() {
        var argument = Tuple.create(
                Map.of("level1_key1", Tuple.create(
                                Map.of("level2_key1", Tuple.create(
                                        Map.of("level3_key1", "level3_value1"))
                                )
                        ),
                        "level1_key2", Tuple.create(
                                Map.of("level2_key1", Tuple.create(
                                        Map.of("level3_key1", "level3_value1"))
                                )
                        ),
                        "level1_key3", "Non-tuple-string-value",
                        "level1_key4", 42
                )
        );

        Tuple resultTuple = client().compute().execute(
                JobTarget.node(node(1)),
                JobDescriptor.builder(TupleComputeJob.class).build(),
                argument
        );

        assertThat(resultTuple, equalTo(argument));
    }

    static class TupleResultJob implements ComputeJob<Integer, Tuple> {
        @Override
        public @Nullable CompletableFuture<Tuple> executeAsync(JobExecutionContext context, @Nullable Integer key) {
            // todo: There is no table for some reason in context.ignite().
            return completedFuture(Tuple.create().set("value_col", "hi"));
        }
    }

    private static class TupleArgJob implements ComputeJob<Tuple, String> {
        @Override
        public @Nullable CompletableFuture<String> executeAsync(JobExecutionContext context, @Nullable Tuple arg) {
            if (arg == null) {
                return completedFuture("null");
            }

            return completedFuture(arg.stringValue("value_col"));
        }
    }

    /** Returns the argument as a result. */
    private static class TupleComputeJob implements ComputeJob<Tuple, Tuple> {
        @Override
        public @Nullable CompletableFuture<Tuple> executeAsync(JobExecutionContext context, @Nullable Tuple arg) {
            return completedFuture(arg);
        }
    }
}
