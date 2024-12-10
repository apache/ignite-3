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
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Test the OOTB support for Tuples in compute api.
 */
public class ItThinClientTupleComputeMarshallingTest extends ClusterPerClassIntegrationTest {
    private static final String TABLE_NAME = "test";

    private static final String COLUMN_KEY = "key_col";

    private static final String COLUMN_VAL = "value_col";

    private IgniteClient client;

    @BeforeAll
    void beforeAll() {
        sql(format("CREATE TABLE {} ({} INT PRIMARY KEY, {} VARCHAR)", TABLE_NAME, COLUMN_KEY, COLUMN_VAL));
        sql(format("INSERT INTO {} ({}, {}) VALUES (2, 'hi')", TABLE_NAME, COLUMN_KEY, COLUMN_VAL));

        String address = "127.0.0.1:" + unwrapIgniteImpl(node(0)).clientAddress().port();
        client = IgniteClient.builder().addresses(address).build();
    }

    @AfterAll
    void afterAll() {
        dropAllTables();

        client.close();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void tupleFromTableApiAsArgument(int targetNodeIdx) {
        // Given target node.
        var targetNode = clusterNode(targetNodeIdx);

        // Given tuple from the table.
        var tup = client.tables().table(TABLE_NAME).keyValueView().get(null, Tuple.create().set(COLUMN_KEY, 2));

        // When execute job with the tuple as an argument.
        String result = client.compute().execute(
                JobTarget.node(targetNode),
                JobDescriptor.builder(TupleArgJob.class).build(),
                tup
        );

        // Then the result is taken from the table.
        assertThat(result, is("hi"));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void tupleFromTableReturned(int targetNodeIdx) {
        // Given target node.
        var targetNode = clusterNode(targetNodeIdx);

        // Given.
        var key = 2;

        // When execute job that returns tuple from the table.
        Tuple result = client.compute().execute(
                JobTarget.node(targetNode),
                JobDescriptor.builder(TupleResultJob.class).build(),
                key
        );

        // Then tuple is returned.
        assertThat(result.stringValue(COLUMN_VAL), is("hi"));
    }

    /**
     * Tests that the nested tuples are correctly serialized and deserialized.
     */
    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void nestedTuplesArgumentSerialization(int targetNodeIdx) {
        // Given target node.
        var targetNode = clusterNode(targetNodeIdx);

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

        Tuple resultTuple = client.compute().execute(
                JobTarget.node(targetNode),
                JobDescriptor.builder(TupleComputeJob.class).build(),
                argument
        );

        assertThat(resultTuple, equalTo(argument));
    }

    static class TupleResultJob implements ComputeJob<Integer, Tuple> {
        @Override
        public @Nullable CompletableFuture<Tuple> executeAsync(JobExecutionContext context, @Nullable Integer key) {
            return context.ignite().tables().tableAsync(TABLE_NAME)
                    .thenApply(table -> table.keyValueView().get(null, Tuple.create().set(COLUMN_KEY, key)));
        }
    }

    private static class TupleArgJob implements ComputeJob<Tuple, String> {
        @Override
        public @Nullable CompletableFuture<String> executeAsync(JobExecutionContext context, @Nullable Tuple arg) {
            if (arg == null) {
                return completedFuture("null");
            }

            return completedFuture(arg.stringValue(COLUMN_VAL));
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
