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

package org.apache.ignite.internal.benchmark;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import org.apache.ignite.table.Tuple;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.runner.RunnerException;

/**
 * Benchmark for a single upsert operation via KV API with a possibility to disable updates via RAFT and to storage.
 */
public class UpsertWithIndexKvBenchmark extends UpsertKvBenchmark {
    private static final String INDEX_CREATE_SQL = "CREATE INDEX " + TABLE_NAME + "_field{}_idx ON " + TABLE_NAME + " USING {} (field{});";

    @Param({"0", "10"})
    private int idxes;

    @Param({"HASH", "SORTED"})
    private String indexType;

    @Param({"uniquePrefix", "uniquePostfix"})
    private String fieldValueGeneration;

    @Param({"100"})
    protected int fieldLength;

    /**
     * Initializes the tuple.
     */
    @Override
    @Setup
    public void setUp() {
        super.setUp();

        StringBuilder sqlScript = new StringBuilder();

        if (idxes > 10) {
            throw new IllegalStateException("Unexpected value of idxes: " + idxes);
        }

        for (int i = 1; i <= idxes; i++) {
            sqlScript.append(format(INDEX_CREATE_SQL, i, indexType, i));
        }

        if (sqlScript.length() > 0) {
            igniteImpl.sql().executeScript(sqlScript.toString());
        }
    }

    @Override
    protected Tuple valueTuple(int id) {
        String formattedString = String.format("%" + (fieldValueGeneration.equals("uniquePrefix") ? '-' : '0') + fieldLength + "d", id);

        String fieldVal = formattedString.length() > fieldLength ? formattedString.substring(0, fieldLength) : formattedString;

        return Tuple.create()
                .set("field1", fieldVal)
                .set("field2", fieldVal)
                .set("field3", fieldVal)
                .set("field4", fieldVal)
                .set("field5", fieldVal)
                .set("field6", fieldVal)
                .set("field7", fieldVal)
                .set("field8", fieldVal)
                .set("field9", fieldVal)
                .set("field10", fieldVal);
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        runBenchmark(UpsertWithIndexKvBenchmark.class, args);
    }
}
