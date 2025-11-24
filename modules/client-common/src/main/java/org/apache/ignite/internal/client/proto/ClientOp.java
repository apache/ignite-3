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

package org.apache.ignite.internal.client.proto;

import java.util.BitSet;

/**
 * Client operation codes.
 */
public class ClientOp {
    /** Heartbeat. */
    public static final int HEARTBEAT = 1;

    /** Get tables. */
    public static final int TABLES_GET = 3;

    /** Get table. */
    public static final int TABLE_GET = 4;

    /** Get schemas. */
    public static final int SCHEMAS_GET = 5;

    /** Upsert tuple. */
    public static final int TUPLE_UPSERT = 10;

    /** Get tuple. */
    public static final int TUPLE_GET = 12;

    /** Upsert all tuples. */
    public static final int TUPLE_UPSERT_ALL = 13;

    /** Get all tuples. */
    public static final int TUPLE_GET_ALL = 15;

    /** Get and upsert tuple. */
    public static final int TUPLE_GET_AND_UPSERT = 16;

    /** Insert tuple. */
    public static final int TUPLE_INSERT = 18;

    /** Insert all tuples. */
    public static final int TUPLE_INSERT_ALL = 20;

    /** Replace tuple. */
    public static final int TUPLE_REPLACE = 22;

    /** Replace exact tuple. */
    public static final int TUPLE_REPLACE_EXACT = 24;

    /** Get and replace tuple. */
    public static final int TUPLE_GET_AND_REPLACE = 26;

    /** Delete tuple. */
    public static final int TUPLE_DELETE = 28;

    /** Delete all tuples. */
    public static final int TUPLE_DELETE_ALL = 29;

    /** Delete exact tuple. */
    public static final int TUPLE_DELETE_EXACT = 30;

    /** Delete all exact tuples. */
    public static final int TUPLE_DELETE_ALL_EXACT = 31;

    /** Get and delete tuple. */
    public static final int TUPLE_GET_AND_DELETE = 32;

    /** Check if the tuple with the given key exists. */
    public static final int TUPLE_CONTAINS_KEY = 33;

    /** Execute sql query. */
    public static final int JDBC_EXEC = 34;

    /** Get next batch by query cursor. */
    public static final int JDBC_NEXT = 35;

    /** Execute batch query. */
    public static final int JDBC_EXEC_BATCH = 36;

    /** Close query cursor. */
    public static final int JDBC_CURSOR_CLOSE = 37;

    /** Get table metadata. */
    public static final int JDBC_TABLE_META = 38;

    /** Get column metadata. */
    public static final int JDBC_COLUMN_META = 39;

    /** Get schemas list. */
    public static final int JDBC_SCHEMAS_META = 40;

    /** Get primary key metadata. */
    public static final int JDBC_PK_META = 41;

    /** Begin transaction. */
    public static final int TX_BEGIN = 43;

    /** Commit transaction. */
    public static final int TX_COMMIT = 44;

    /** Rollback transaction. */
    public static final int TX_ROLLBACK = 45;

    /** Execute prepared statement batch query. */
    public static final int JDBC_SQL_EXEC_PS_BATCH = 46;

    /** Execute compute job. */
    public static final int COMPUTE_EXECUTE = 47;

    /** Get cluster nodes. */
    public static final int CLUSTER_GET_NODES = 48;

    /** Execute compute job. */
    public static final int COMPUTE_EXECUTE_COLOCATED = 49;

    /** Execute SQL query. */
    public static final int SQL_EXEC = 50;

    /** Get next page. */
    public static final int SQL_CURSOR_NEXT_PAGE = 51;

    /** Close cursor. */
    public static final int SQL_CURSOR_CLOSE = 52;

    /** Get partition assignment. */
    public static final int PARTITION_ASSIGNMENT_GET = 53;

    /** Create JDBC connection context on a server. */
    public static final int JDBC_CONNECT = 54;

    /** JDBC command to commit/rollback transaction. */
    public static final int JDBC_TX_FINISH = 55;

    /** Execute SQL script. */
    public static final int SQL_EXEC_SCRIPT = 56;

    /** SQL query metadata. */
    public static final int SQL_QUERY_META = 57;

    /** JDBC get more results command. */
    public static final int JDBC_MORE_RESULTS = 58;

    /** Get compute job state. */
    public static final int COMPUTE_GET_STATE = 59;

    /** Cancel compute job. */
    public static final int COMPUTE_CANCEL = 60;

    /** Change compute job priority. */
    public static final int COMPUTE_CHANGE_PRIORITY = 61;

    /** Send streamer batch. */
    public static final int STREAMER_BATCH_SEND = 62;

    /** Execute SQL query with the parameters batch. */
    public static final int SQL_EXEC_BATCH = 63;

    /** Execute MapReduce task. */
    public static final int COMPUTE_EXECUTE_MAPREDUCE = 64;

    /** Get all primary replicas mapping to cluster nodes. */
    public static final int PRIMARY_REPLICAS_GET = 65;

    /** Send streamer batch with receiver. */
    public static final int STREAMER_WITH_RECEIVER_BATCH_SEND = 66;

    /** Check if all tuples with the given keys collection exist. */
    public static final int TUPLE_CONTAINS_ALL_KEYS = 67;

    /** Cancels the execution of JDBC statement. */
    public static final int JDBC_CANCEL = 68;

    /** Execute partitioned compute job. */
    public static final int COMPUTE_EXECUTE_PARTITIONED = 69;

    /** Cancel execution of a request previously initiated on the same connection. */
    public static final int OPERATION_CANCEL = 70;

    /** Get tables (returns Qualified names). */
    public static final int TABLES_GET_QUALIFIED = 71;

    /** Get table by qualified names (returns Qualified names). */
    public static final int TABLE_GET_QUALIFIED = 72;

    /** Response to a server->client operation. */
    public static final int SERVER_OP_RESPONSE = 73;

    /** Get next result set. */
    public static final int SQL_CURSOR_NEXT_RESULT_SET = 74;

    /** Reserved for extensions: min. */
    @SuppressWarnings("unused")
    public static final int RESERVED_EXTENSION_RANGE_START = 1000;

    /** Reserved for extensions: max. */
    @SuppressWarnings("unused")
    public static final int RESERVED_EXTENSION_RANGE_END = 2000;

    /** Write mask. */
    private static final BitSet WRITE_MASK = new BitSet(64);

    /** Batch mask. */
    private static final BitSet BATCH_MASK = new BitSet(64);

    /** Partition operation mask. */
    private static final BitSet OP_MASK = new BitSet(64);

    static {
        WRITE_MASK.set(TUPLE_UPSERT);
        WRITE_MASK.set(TUPLE_GET_AND_UPSERT);
        WRITE_MASK.set(TUPLE_INSERT);
        WRITE_MASK.set(TUPLE_REPLACE);
        WRITE_MASK.set(TUPLE_REPLACE_EXACT);
        WRITE_MASK.set(TUPLE_GET_AND_REPLACE);
        WRITE_MASK.set(TUPLE_DELETE);
        WRITE_MASK.set(TUPLE_DELETE_EXACT);
        WRITE_MASK.set(TUPLE_GET_AND_DELETE);
        WRITE_MASK.set(TUPLE_UPSERT_ALL);
        WRITE_MASK.set(TUPLE_INSERT_ALL);
        WRITE_MASK.set(TUPLE_DELETE_ALL);
        WRITE_MASK.set(TUPLE_DELETE_ALL_EXACT);

        BATCH_MASK.set(TUPLE_CONTAINS_ALL_KEYS);
        BATCH_MASK.set(TUPLE_GET_ALL);
        BATCH_MASK.set(TUPLE_UPSERT_ALL);
        BATCH_MASK.set(TUPLE_INSERT_ALL);
        BATCH_MASK.set(TUPLE_DELETE_ALL);
        BATCH_MASK.set(TUPLE_DELETE_ALL_EXACT);

        OP_MASK.set(TABLES_GET);
        OP_MASK.set(TUPLE_UPSERT);
        OP_MASK.set(TUPLE_GET);
        OP_MASK.set(TUPLE_GET_AND_UPSERT);
        OP_MASK.set(TUPLE_INSERT);
        OP_MASK.set(TUPLE_REPLACE);
        OP_MASK.set(TUPLE_REPLACE_EXACT);
        OP_MASK.set(TUPLE_GET_AND_REPLACE);
        OP_MASK.set(TUPLE_DELETE);
        OP_MASK.set(TUPLE_DELETE_EXACT);
        OP_MASK.set(TUPLE_GET_AND_DELETE);
        OP_MASK.set(TUPLE_CONTAINS_KEY);
        OP_MASK.set(STREAMER_BATCH_SEND);
        OP_MASK.set(TX_COMMIT);
        OP_MASK.set(TX_ROLLBACK);
        OP_MASK.set(TUPLE_GET_ALL);
        OP_MASK.set(TUPLE_CONTAINS_ALL_KEYS);
    }

    /**
     * Test if the operation is a write.
     *
     * @param opCode The operation code.
     * @return The status.
     */
    public static boolean isWrite(int opCode) {
        return WRITE_MASK.get(opCode);
    }

    /**
     * Test if the operation is a batch.
     *
     * @param opCode The operation code.
     * @return The status.
     */
    public static boolean isBatch(int opCode) {
        return BATCH_MASK.get(opCode);
    }

    /**
     * Test if the partition operation.
     *
     * @param opCode The operation code.
     * @return The status.
     */
    public static boolean isPartitionOperation(int opCode) {
        // Sql-related operation must do some bookkeeping first on the client's thread to avoid races
        // (for instance, cancellation must not be processed until execution request is registered).
        // || opCode == ClientOp.SQL_EXEC
        // || opCode == ClientOp.SQL_EXEC_BATCH
        // || opCode == ClientOp.SQL_EXEC_SCRIPT
        // || opCode == ClientOp.SQL_QUERY_META;

        // TODO: IGNITE-23641 The batch operations were excluded because fast switching leads to performance degradation for them.
        // || opCode == ClientOp.TUPLE_UPSERT_ALL
        // || opCode == ClientOp.TUPLE_GET_ALL
        // || opCode == ClientOp.TUPLE_INSERT_ALL
        // || opCode == ClientOp.TUPLE_DELETE_ALL
        // || opCode == ClientOp.TUPLE_DELETE_ALL_EXACT
        // || opCode == ClientOp.TUPLE_CONTAINS_ALL_KEYS;
        return OP_MASK.get(opCode);
    }
}
